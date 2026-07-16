# --- build ---
FROM rust:1.88-slim AS build
# .cargo/config.toml links x86_64-linux with clang + mold; the slim image ships neither.
RUN apt-get update && apt-get install -y --no-install-recommends clang mold && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release --bin stroma-serve --bin stroma --bin stroma-mcp

# --- runtime ---
FROM debian:bookworm-slim
LABEL org.opencontainers.image.source="https://github.com/katsut/stromadb" \
      org.opencontainers.image.url="https://stromadb.com" \
      org.opencontainers.image.description="Real-time GraphRAG engine for LLMs — typed graph x vectors x bitemporal time. Source-available." \
      org.opencontainers.image.licenses="Elastic-2.0"
RUN useradd -u 10001 -m stroma && mkdir -p /data && chown stroma /data
COPY --from=build /src/target/release/stroma-serve /usr/local/bin/
COPY --from=build /src/target/release/stroma       /usr/local/bin/
COPY --from=build /src/target/release/stroma-mcp    /usr/local/bin/
USER stroma
# A fresh /data volume is initialized on first run (stroma-serve calls open_or_init).
ENV STROMA_DB=/data \
    STROMA_ADDR=0.0.0.0:7687
VOLUME /data
EXPOSE 7687
ENTRYPOINT ["stroma-serve"]
