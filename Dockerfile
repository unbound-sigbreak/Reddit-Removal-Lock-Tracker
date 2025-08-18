
FROM node:22.12.0-bookworm

WORKDIR /app

COPY ./src/* ./src/
COPY ./index.js ./
COPY ./package.json ./
COPY ./package-lock.json ./
# COPY ./node_modules ./node_modules/

RUN npm install

CMD ["node", "index.js"]
