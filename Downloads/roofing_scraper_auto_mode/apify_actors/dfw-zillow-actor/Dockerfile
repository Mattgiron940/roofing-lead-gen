FROM apify/actor-node:18

COPY package*.json ./

RUN npm ci --only=prod --no-optional

COPY . ./

CMD npm start