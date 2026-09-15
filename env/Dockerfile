# Multi-process compose uses one image with different commands.
FROM node:20-alpine
WORKDIR /app
# Zero runtime dependencies: only project sources are copied.
COPY package.json ./
COPY src ./src
COPY bin ./bin
RUN chmod +x bin/bridgectl.js
ENV NODE_ENV=production
# Default command overridden per service in docker-compose.yml
CMD ["node", "bin/start-all.js"]
