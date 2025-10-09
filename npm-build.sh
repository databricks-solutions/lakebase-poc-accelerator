#!/bin/bash
# Build frontend
echo "📦 Building frontend..."
cd app/frontend
echo "🔄 Installing frontend dependencies..."
npm install
echo "🔄 Building frontend..."
npm run build
echo "🔄 Copying build folder to root..."
# Copy the build folder to the project root, replacing any existing build folder
if [ -d "../../build" ]; then
  echo "🗑️ Removing existing build folder in root..."
  rm -rf ../../build
fi

echo "📁 Copying new build folder to root..."
cp -r build ../../build
echo "✅ Build folder copied to root."
echo "🗑️ Removing build app/frontend/build folder"
rm -rf build

# Go back to root
cd ../..
