FROM node:20-bookworm-slim

WORKDIR /app
COPY . .
WORKDIR /app/examples/signalsbot
RUN npm install
RUN mkdir -p /data
ENV SIGNALS_DB_PATH=/data/signalsbot.json
ENTRYPOINT ["node", "src/main.js"]
CMD ["papertrader"]
