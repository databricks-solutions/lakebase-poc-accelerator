#!/bin/bash
# Fix and deploy to Databricks Apps

set -e

echo "🔧 Deploying to Databricks Apps..."

# Build frontend
echo "📦 Building frontend..."
cd app/frontend
npm install
npm run build
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

echo "🔄 Syncing to Databricks..."
databricks sync . /Workspace/Users/anhhoang.chu@databricks.com/lakebase-accelerator-ak

echo "🚀 Deploying to Databricks Apps..."
databricks apps deploy lakebase-accelerator-ak --source-code-path /Workspace/Users/anhhoang.chu@databricks.com/lakebase-accelerator-ak

echo "✅ Fix deployed! Check the logs for debug information."
