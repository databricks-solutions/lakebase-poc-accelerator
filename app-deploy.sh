#!/bin/bash
# Deploy to Databricks Apps

echo "🔄 Syncing to Databricks Full..."
databricks sync . /Workspace/Users/anhhoang.chu@databricks.com/lakebase-accelerator-ak --full

echo "🚀 Deploying to Databricks Apps..."
databricks apps deploy lakebase-accelerator-ak --source-code-path /Workspace/Users/anhhoang.chu@databricks.com/lakebase-accelerator-ak

echo "✅ Fix deployed! Check the logs for debug information."
