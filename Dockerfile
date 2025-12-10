FROM node:25-alpine

# Install lftp and openssh-client for sftp support
RUN apk add --no-cache lftp openssh-client bash

# Create app directory
WORKDIR /app

# Copy the sync script
COPY main.js . 

# Create a non-root user (optional, remove if you need root for file permissions)
# RUN addgroup -S syncuser && adduser -S syncuser -G syncuser
# USER syncuser

# Default environment variables
ENV RETENTION_MINUTES=2880
ENV NODE_ENV=production

# Run the sync service
CMD ["node", "main.js"]
