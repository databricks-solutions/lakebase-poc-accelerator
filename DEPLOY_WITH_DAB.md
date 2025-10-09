# Deploy Lakebase Accelerator App with Databricks Asset Bundles

Simple guide to deploy your Lakebase POC Accelerator app using DAB.

## Prerequisites

- Databricks CLI installed and authenticated, follow instruction in [README.md](README.md)
- Node.js and npm installed

---

## Step 1: Build Frontend

Build the React frontend and copy it to the root folder:

```bash
# Run the build script
./npm-build.sh
```

Or manually:

```bash
cd app/frontend
npm install
npm run build

# Copy build to root
cp -r build ../../build
rm -rf build
cd ../..
```

**Result:** You should now have a `build/` folder at the project root containing the compiled frontend.


---

## Step 2: Configure variables

Edit the host name in `databricks.yml`:

```yaml
# Deployment targets
targets:
  # Development environment
  dev:
    mode: development
    default: true
    workspace:
      host: https://company-test.cloud.databricks.com # Change to your Databricks workspace host
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
  
```

Edit the app name in `resources/app_deployment.yml`:

```yaml
resources:
  apps:
    lakebase_accelerator_app:
      name: <your-app-name>     # Change this to your desired app name
      description: "Lakebase POC Accelerator"
      source_code_path: .
      permissions:
        - level: CAN_USE
          group_name: users
```

**Note:** App names must be unique in your workspace and contain only lowercase alphanumeric characters and hyphens.

---

## Step 3: Deploy with DAB

Run these commands from the **project root**:


### Validate Bundle Configuration

```bash
databricks bundle validate
```

This checks your configuration for errors. You should see:
```
✓ Validation OK!
```

### Deploy the Bundle

**If `build` is specified in .gitignore. Remove `build` from .gitignore**

Specify the target environment with --target flag (dev is default)

```bash
databricks bundle deploy --target dev
```

This will:
- Upload all source code to the workspace
- Create the Databricks App
- Link the code to the app

**Note:** The app will be created in STOPPED state.

### Start and Deploy Source Code to the App 

```bash
databricks bundle run lakebase_accelerator_app
```

Or use the apps CLI:

```bash
databricks apps start <your-app-name>
```

**Wait 3-5 minutes** for the app compute to start.

---

## Step 4: Access Your App

Get the app URL:

```bash
databricks apps get <your-app-name>
```

Or check in the UI:
1. Go to **Compute → Apps**
2. Find your app
3. Click to open

---

## Quick Deploy (All Steps)

```bash
# 1. Build frontend
./npm-build.sh

# 2. Deploy
databricks bundle validate
databricks bundle deploy
databricks bundle run lakebase_accelerator_app

# 3. Get URL
databricks apps get <your-app-name>
```

---

## Useful Commands

### Check App Status
```bash
databricks apps get <your-app-name>
```

### Start/Stop App
```bash
# Start
databricks apps start <your-app-name>

# Stop (to save costs)
databricks apps stop <your-app-name>
```

### After Update Code
```bash
# After making code changes:
# 1. Rebuild frontend (if changed)
./npm-build.sh

# 2. Redeploy
databricks bundle deploy
databricks bundle run lakebase_accelerator_app

# 3. Restart app
databricks apps stop <your-app-name>
databricks apps start <your-app-name>
```

### Delete Everything
```bash
databricks bundle destroy --auto-approve
```

---

## Deployment Targets

Deploy to different environments:

### Development (default)
```bash
databricks bundle deploy --target dev
```

### Test
```bash
databricks bundle deploy --target test
```

### Production
```bash
databricks bundle deploy --target prod
```

---

## Troubleshooting

### Error: "source_code_path must be set"
Make sure `source_code_path: .` is set in `resources/app_deployment.yml`

### Error: "app.yml not found"
The `app.yml` file must be at the project root (same level as `databricks.yml`)

### Error: "build folder not found"
Run `./npm-build.sh` to build the frontend first. **If `build` is specified in .gitignore, remove `build` from .gitignore**

## Project Structure

```
lakebase-poc-accelerator/          ← Root (where you run commands)
├── databricks.yml                 ← Bundle configuration
└── resources/
│   └── app_deployment.yml         ← App resource definition
├── app.yml                        ← App runtime config (command, env vars)
├── requirements.txt               ← Python dependencies
├── build/                         ← Frontend build (created by npm-build.sh)
├── app/
│   ├── backend/                   ← FastAPI backend
│   ├── frontend/                  ← React source code
│   ├── notebooks/                 ← Jupyter notebooks for pgbench run
└── app.py                         ← Main FastAPI application
```

---

## What Gets Deployed

When you run `databricks bundle deploy`, these files are uploaded to `/Workspace/Users/<your-email>/.bundle/lakebase_accelerator/dev/files/`:

✅ `app.yml` - App configuration  
✅ `requirements.txt` - Python dependencies  
✅ `build/` - Compiled frontend  
✅ `app/` - All application code  
✅ `app.py` - Main application entry point  

---

## App Lifecycle

```
BUILD → DEPLOY → START → RUNNING
  ↓       ↓        ↓        ↓
npm    bundle   bundle    Access
build  deploy   run       URL
```

🎉 Done!
