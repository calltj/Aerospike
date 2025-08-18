# Use a recent Node.js image with modern glibc
FROM node:22-bullseye

# Set working directory
WORKDIR /app

# Copy package files and install dependencies
COPY package*.json ./
RUN npm install

# Copy the rest of your code
COPY . .

# Expose your app's port
EXPOSE 3000

# Start your app
CMD ["node", "src/index.js"]