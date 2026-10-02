//! Per-fact access labels at read time: the label mask and the masked read view.
//!
//! A fact's version row may carry an access label, and a predicate may declare a label floor. The
//! effective label of a row is the greater of the two (labels are numbered by sensitivity, so the
//! stricter one wins). A row is visible to a principal whose `allowed_labels` bitmask allows both
//! the floor and the row's own label; under the intended tier masks (every label up to some level)
//! that is exactly "the effective label is allowed". A row with neither (unlabeled, on a predicate
//! without a floor) is always visible. The floor is read from the catalog at read time, so changing a floor
//! re-labels every existing row of the predicate without rewriting history.
//!
//! [`Masked`] is the one place this is applied. It wraps the pinned [`Snapshot`] and answers every
//! fact read ([`Facts`]) exactly as the snapshot of a store in which the hidden rows were never
//! written would answer it: a hidden head row gives way to the latest visible row, a hidden
//! element drops out of its set, a hidden version drops out of every timeline, and a key whose
//! rows are all hidden reads as never written. Node attributes (type, node label) are not facts
//! and pass through unchanged. The read primitives in [`crate::query`] are written against
//! [`Facts`], so the same code serves masked and unmasked reads.
//!
//! Cost: when the mask hides nothing that exists ([`FactMask::hides_any`] — every stored label and
//! every declared floor is allowed), every method is a direct borrow of the snapshot. Otherwise
//! only keys whose rows carry a hidden label, or whose predicate has a hidden floor, are
//! recomputed, and only when read.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::RangeBounds;
use std::sync::Arc;

use crate::catalog::Catalog;
use crate::fact::{FieldId, NodeId};
use crate::fold::{ObjKey, Snapshot, VersionRow};
use crate::hash::FxHashMap;

/// The greatest valid access label: `allowed_labels` is a 32-bit mask, one bit per label.
pub const MAX_LABEL: u8 = 31;

/// A fact key: `(subject, predicate)`.
pub type Key = (NodeId, FieldId);

/// Whether `label` is permitted by the bitmask `allowed`.
pub fn label_allowed(allowed: u32, label: u8) -> bool {
    label <= MAX_LABEL && (allowed >> label) & 1 == 1
}

/// The effective label of a row: the greater of the predicate's floor and the row's own label.
pub fn effective_label(floor: Option<u8>, label: Option<u8>) -> Option<u8> {
    floor.max(label)
}

/// A principal's label mask resolved against the catalog's predicate floors.
#[derive(Clone, Debug)]
pub struct FactMask {
    allowed: u32,
    floors: FxHashMap<FieldId, u8>,
}

impl FactMask {
    /// The mask for `allowed` under the floors `cat` currently declares.
    pub fn new(cat: &Catalog, allowed: u32) -> Self {
        let floors = cat
            .predicates()
            .filter_map(|d| d.label_floor.map(|f| (d.id, f)))
            .collect();
        FactMask { allowed, floors }
    }

    /// The bitmask this mask allows.
    pub fn allowed(&self) -> u32 {
        self.allowed
    }

    /// The label floor declared on predicate `p`, if any.
    pub fn floor(&self, p: FieldId) -> Option<u8> {
        self.floors.get(&p).copied()
    }

    /// Whether every fact of predicate `p` is hidden by its floor.
    pub fn predicate_hidden(&self, p: FieldId) -> bool {
        self.floor(p)
            .is_some_and(|f| !label_allowed(self.allowed, f))
    }

    /// Whether a row of predicate `p` written with `label` is visible: both the floor and the
    /// row's own label, where present, are allowed. Under a mask that allows every label up to some
    /// level (labels used as ordered tiers) this is exactly "the effective label is allowed"; under
    /// an arbitrary mask it is the stricter reading, so labeling a row above a hidden floor never
    /// makes it visible.
    pub fn row_visible(&self, p: FieldId, label: Option<u8>) -> bool {
        let ok = |l: Option<u8>| l.is_none_or(|l| label_allowed(self.allowed, l));
        ok(self.floor(p)) && ok(label)
    }

    /// Whether this mask hides any row `snap` holds or could hold: some stored label or some
    /// declared floor is not allowed. `false` means every read through the mask equals the
    /// unmasked read.
    pub fn hides_any(&self, snap: &Snapshot) -> bool {
        snap.fact_label_counts
            .keys()
            .any(|&l| !label_allowed(self.allowed, l))
            || self
                .floors
                .values()
                .any(|&f| !label_allowed(self.allowed, f))
    }

    /// Whether the mask changes what a read of key `k` sees: the predicate's floor hides it, or at
    /// least one of its live rows has a hidden effective label. Conformance uses this to tell a
    /// verdict that depends on a hidden fact (see [`crate::conformance`]).
    pub fn key_affected(&self, snap: &Snapshot, k: &Key) -> bool {
        self.predicate_hidden(k.1)
            || snap
                .fact_labels
                .get(k)
                .is_some_and(|rows| rows.values().any(|&l| !self.row_visible(k.1, Some(l))))
    }
}

/// Read access to fact state. Implemented by [`Snapshot`] (every row, unmasked) and by [`Masked`]
/// (only the rows a label mask allows). Values follow the snapshot's shapes: a One key's rows are
/// its live version rows ascending by order key and its value is the last row's object (`None`
/// inside = the winner is a close); a Many key's set is its present elements, its rows the
/// per-element version rows. A key with nothing visible reads as `None`.
pub trait Facts {
    /// The underlying snapshot — for node attributes, which no fact label masks.
    fn snapshot(&self) -> &Snapshot;
    /// Live version rows of a One key.
    fn one_rows(&self, k: &Key) -> Option<Cow<'_, [VersionRow]>>;
    /// Current value of a One key.
    fn one_value(&self, k: &Key) -> Option<Cow<'_, Option<ObjKey>>>;
    /// Present element set of a Many key (never empty when `Some`).
    fn many_set(&self, k: &Key) -> Option<Cow<'_, BTreeSet<ObjKey>>>;
    /// Per-element version rows of a Many key.
    fn many_rows(&self, k: &Key) -> Option<Cow<'_, BTreeMap<ObjKey, Vec<VersionRow>>>>;
    /// Properties of the edge `(k, object)`.
    fn edge_props_of(&self, k: &Key, object: &ObjKey) -> Option<&BTreeMap<String, ObjKey>>;
    /// Visit the current One values of the keys in `range`, ascending.
    fn for_each_one(&self, range: impl RangeBounds<Key>, f: impl FnMut(&Key, &Option<ObjKey>));
    /// Visit the present Many sets of the keys in `range`, ascending.
    fn for_each_many(&self, range: impl RangeBounds<Key>, f: impl FnMut(&Key, &BTreeSet<ObjKey>));
    /// Visit every One key with at least one row, ascending.
    fn for_each_one_key(&self, f: impl FnMut(&Key));
    /// Visit every Many key with at least one element row, ascending.
    fn for_each_many_key(&self, f: impl FnMut(&Key));
}

impl Facts for Snapshot {
    fn snapshot(&self) -> &Snapshot {
        self
    }
    fn one_rows(&self, k: &Key) -> Option<Cow<'_, [VersionRow]>> {
        self.one_history
            .get(k)
            .map(|rows| Cow::Borrowed(rows.as_slice()))
    }
    fn one_value(&self, k: &Key) -> Option<Cow<'_, Option<ObjKey>>> {
        self.one.get(k).map(Cow::Borrowed)
    }
    fn many_set(&self, k: &Key) -> Option<Cow<'_, BTreeSet<ObjKey>>> {
        self.many.get(k).map(Cow::Borrowed)
    }
    fn many_rows(&self, k: &Key) -> Option<Cow<'_, BTreeMap<ObjKey, Vec<VersionRow>>>> {
        self.many_history.get(k).map(Cow::Borrowed)
    }
    fn edge_props_of(&self, k: &Key, object: &ObjKey) -> Option<&BTreeMap<String, ObjKey>> {
        self.edge_props.get(k)?.get(object)
    }
    fn for_each_one(&self, range: impl RangeBounds<Key>, mut f: impl FnMut(&Key, &Option<ObjKey>)) {
        for (k, v) in self.one.range(range) {
            f(k, v);
        }
    }
    fn for_each_many(
        &self,
        range: impl RangeBounds<Key>,
        mut f: impl FnMut(&Key, &BTreeSet<ObjKey>),
    ) {
        for (k, v) in self.many.range(range) {
            f(k, v);
        }
    }
    fn for_each_one_key(&self, mut f: impl FnMut(&Key)) {
        for k in self.one_history.keys() {
            f(k);
        }
    }
    fn for_each_many_key(&self, mut f: impl FnMut(&Key)) {
        for k in self.many_history.keys() {
            f(k);
        }
    }
}

impl<T: Facts> Facts for Arc<T> {
    fn snapshot(&self) -> &Snapshot {
        (**self).snapshot()
    }
    fn one_rows(&self, k: &Key) -> Option<Cow<'_, [VersionRow]>> {
        (**self).one_rows(k)
    }
    fn one_value(&self, k: &Key) -> Option<Cow<'_, Option<ObjKey>>> {
        (**self).one_value(k)
    }
    fn many_set(&self, k: &Key) -> Option<Cow<'_, BTreeSet<ObjKey>>> {
        (**self).many_set(k)
    }
    fn many_rows(&self, k: &Key) -> Option<Cow<'_, BTreeMap<ObjKey, Vec<VersionRow>>>> {
        (**self).many_rows(k)
    }
    fn edge_props_of(&self, k: &Key, object: &ObjKey) -> Option<&BTreeMap<String, ObjKey>> {
        (**self).edge_props_of(k, object)
    }
    fn for_each_one(&self, range: impl RangeBounds<Key>, f: impl FnMut(&Key, &Option<ObjKey>)) {
        (**self).for_each_one(range, f)
    }
    fn for_each_many(&self, range: impl RangeBounds<Key>, f: impl FnMut(&Key, &BTreeSet<ObjKey>)) {
        (**self).for_each_many(range, f)
    }
    fn for_each_one_key(&self, f: impl FnMut(&Key)) {
        (**self).for_each_one_key(f)
    }
    fn for_each_many_key(&self, f: impl FnMut(&Key)) {
        (**self).for_each_many_key(f)
    }
}

/// A snapshot read through a label mask — see the module docs.
pub struct Masked<'a> {
    snap: &'a Snapshot,
    mask: FactMask,
    /// [`FactMask::hides_any`] for this snapshot: `false` makes every read a plain borrow.
    active: bool,
}

impl<'a> Masked<'a> {
    /// `snap` as seen by a principal with `allowed`, under the floors `cat` declares.
    pub fn new(snap: &'a Snapshot, cat: &Catalog, allowed: u32) -> Self {
        let mask = FactMask::new(cat, allowed);
        let active = mask.hides_any(snap);
        Masked { snap, mask, active }
    }

    /// The mask this view applies.
    pub fn mask(&self) -> &FactMask {
        &self.mask
    }

    /// Whether the view hides anything at all (`false` = identical to the snapshot).
    pub fn hides_any(&self) -> bool {
        self.active
    }

    /// Whether the mask changes what a read of `k` sees.
    pub fn affected(&self, k: &Key) -> bool {
        self.active && self.mask.key_affected(self.snap, k)
    }

    fn visible(&self, k: &Key, rows: &[VersionRow]) -> Vec<VersionRow> {
        let labels = self.snap.fact_labels.get(k);
        rows.iter()
            .filter(|(ok, ..)| {
                self.mask
                    .row_visible(k.1, labels.and_then(|m| m.get(ok)).copied())
            })
            .cloned()
            .collect()
    }
}

impl Facts for Masked<'_> {
    fn snapshot(&self) -> &Snapshot {
        self.snap
    }

    fn one_rows(&self, k: &Key) -> Option<Cow<'_, [VersionRow]>> {
        let rows = self.snap.one_history.get(k)?;
        if !self.affected(k) {
            return Some(Cow::Borrowed(rows.as_slice()));
        }
        let vis = self.visible(k, rows);
        (!vis.is_empty()).then_some(Cow::Owned(vis))
    }

    fn one_value(&self, k: &Key) -> Option<Cow<'_, Option<ObjKey>>> {
        if !self.affected(k) {
            return self.snap.one.get(k).map(Cow::Borrowed);
        }
        let rows = self.one_rows(k)?;
        rows.last().map(|(_, obj, ..)| Cow::Owned(obj.clone()))
    }

    fn many_set(&self, k: &Key) -> Option<Cow<'_, BTreeSet<ObjKey>>> {
        if !self.affected(k) {
            return self.snap.many.get(k).map(Cow::Borrowed);
        }
        let rows = self.many_rows(k)?;
        let present: BTreeSet<ObjKey> = rows
            .iter()
            .filter(|(_, r)| r.last().is_some_and(|(_, obj, ..)| obj.is_some()))
            .map(|(o, _)| o.clone())
            .collect();
        (!present.is_empty()).then_some(Cow::Owned(present))
    }

    fn many_rows(&self, k: &Key) -> Option<Cow<'_, BTreeMap<ObjKey, Vec<VersionRow>>>> {
        let elems = self.snap.many_history.get(k)?;
        if !self.affected(k) {
            return Some(Cow::Borrowed(elems));
        }
        let out: BTreeMap<ObjKey, Vec<VersionRow>> = elems
            .iter()
            .filter_map(|(o, rows)| {
                let vis = self.visible(k, rows);
                (!vis.is_empty()).then(|| (o.clone(), vis))
            })
            .collect();
        (!out.is_empty()).then_some(Cow::Owned(out))
    }

    /// An edge's properties follow the edge: on a key the mask affects they are visible only while
    /// the edge `(k, object)` has at least one visible row.
    fn edge_props_of(&self, k: &Key, object: &ObjKey) -> Option<&BTreeMap<String, ObjKey>> {
        let props = self.snap.edge_props.get(k)?.get(object)?;
        if !self.affected(k) {
            return Some(props);
        }
        let in_one = self
            .one_rows(k)
            .is_some_and(|rows| rows.iter().any(|(_, o, ..)| o.as_ref() == Some(object)));
        let in_many = self.many_rows(k).is_some_and(|m| m.contains_key(object));
        (in_one || in_many).then_some(props)
    }

    fn for_each_one(&self, range: impl RangeBounds<Key>, mut f: impl FnMut(&Key, &Option<ObjKey>)) {
        for (k, v) in self.snap.one.range(range) {
            if !self.affected(k) {
                f(k, v);
            } else if let Some(v) = self.one_value(k) {
                f(k, &v);
            }
        }
    }

    fn for_each_many(
        &self,
        range: impl RangeBounds<Key>,
        mut f: impl FnMut(&Key, &BTreeSet<ObjKey>),
    ) {
        if !self.active {
            for (k, v) in self.snap.many.range(range) {
                f(k, v);
            }
            return;
        }
        // Walk the history keys, not the present sets: hiding a close can make an element present
        // that the unmasked set omits (and a key with no unmasked present set may gain one).
        for k in self.snap.many_history.range(range).map(|(k, _)| k) {
            if !self.affected(k) {
                if let Some(v) = self.snap.many.get(k) {
                    f(k, v);
                }
            } else if let Some(v) = self.many_set(k) {
                f(k, &v);
            }
        }
    }

    fn for_each_one_key(&self, mut f: impl FnMut(&Key)) {
        for k in self.snap.one_history.keys() {
            if !self.affected(k) || self.one_rows(k).is_some() {
                f(k);
            }
        }
    }

    fn for_each_many_key(&self, mut f: impl FnMut(&Key)) {
        for k in self.snap.many_history.keys() {
            if !self.affected(k) || self.many_rows(k).is_some() {
                f(k);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Cardinality, Range, RelProps, ValueType};
    use crate::fold::{Op, OrderKey, fold_labeled};
    use crate::query;

    fn ok(seq: u64) -> OrderKey {
        OrderKey {
            tx: seq,
            source: 0,
            seq,
        }
    }

    fn set(s: NodeId, p: FieldId, v: &str, vf: i64, seq: u64) -> Op {
        Op::SetOne {
            subject: s,
            predicate: p,
            object: ObjKey::Text(v.into()),
            valid_from: vf,
            valid_to: None,
            ok: ok(seq),
        }
    }

    fn cat() -> (Catalog, FieldId) {
        let mut c = Catalog::new();
        let person = c.register_type("Person");
        let email = c.register_predicate(
            "email",
            Cardinality::One,
            RelProps::default(),
            person,
            Range::Value(ValueType::Text),
        );
        (c, email)
    }

    #[test]
    fn a_hidden_head_gives_way_to_the_latest_visible_row() {
        let (c, email) = cat();
        let snap = fold_labeled(&[
            (set(1, email, "old@example.com", 10, 1), None),
            (set(1, email, "new@example.com", 20, 2), Some(3)),
        ])
        .observe();
        let all = Masked::new(&snap, &c, u32::MAX);
        assert!(!all.hides_any());
        assert_eq!(
            query::point_one(&all, 1, email),
            Some(ObjKey::Text("new@example.com".into()))
        );
        let public = Masked::new(&snap, &c, 0b1);
        assert!(public.hides_any());
        assert_eq!(
            query::point_one(&public, 1, email),
            Some(ObjKey::Text("old@example.com".into()))
        );
        assert_eq!(query::point_one_timeline(&public, 1, email).len(), 1);
        assert_eq!(
            query::point_one_asof(&public, 1, email, 25),
            query::point_one(&public, 1, email)
        );
    }

    #[test]
    fn a_floor_hides_every_row_and_combines_with_row_labels_by_max() {
        let (mut c, email) = cat();
        let snap = fold_labeled(&[
            (set(1, email, "a@example.com", 0, 1), None),
            (set(2, email, "b@example.com", 0, 2), Some(4)),
        ])
        .observe();
        c.set_label_floor(email, Some(2));
        // bit 2 allowed: the floor admits the unlabeled row, but row 2's own label 4 is stricter
        let m = Masked::new(&snap, &c, 0b101);
        assert!(query::point_one(&m, 1, email).is_some());
        assert!(query::point_one(&m, 2, email).is_none());
        // bit 2 not allowed: the floor hides both
        let m = Masked::new(&snap, &c, 0b1 | (1 << 4));
        assert!(query::point_one(&m, 1, email).is_none());
        assert!(query::point_one(&m, 2, email).is_none());
        assert!(m.mask().predicate_hidden(email));
        // a floor above the row label wins: floor 5 over row label 4
        c.set_label_floor(email, Some(5));
        let m = Masked::new(&snap, &c, 1 << 4);
        assert!(query::point_one(&m, 2, email).is_none());
        let m = Masked::new(&snap, &c, (1 << 5) | (1 << 4));
        assert!(query::point_one(&m, 2, email).is_some());
        // under a mask that is not a tier (5 without 4) both labels must be allowed
        let m = Masked::new(&snap, &c, 1 << 5);
        assert!(query::point_one(&m, 1, email).is_some());
        assert!(query::point_one(&m, 2, email).is_none());
        assert_eq!(effective_label(m.mask().floor(email), Some(4)), Some(5));
    }
}
