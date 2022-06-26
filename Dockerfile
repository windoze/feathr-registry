FROM node:16-alpine as ui-builder
WORKDIR /usr/src
RUN apk add --no-cache git && git clone https://github.com/linkedin/feathr.git && cd /usr/src/feathr/ui && npm install && REACT_APP_AAD_APP_CLIENT_ID="1502bdf6-0000-46ed-8c6f-877fc0588bfc" REACT_APP_API_ENDPOINT="" npm run build

FROM messense/rust-musl-cross:x86_64-musl AS builder
WORKDIR /usr/src/
COPY . ./
RUN cargo build --release --target=x86_64-unknown-linux-musl

# Bundle Stage
FROM alpine
RUN apk add --update openssl bash
COPY --from=builder /usr/src/target/x86_64-unknown-linux-musl/release/feathr-registry /app/feathr-registry
COPY --from=ui-builder /usr/src/feathr/ui/build/ /app/static-files
# USER 1000
WORKDIR /app
CMD ["/app/feathr-registry"]
