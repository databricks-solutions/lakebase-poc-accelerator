# DATABRICKS LAKEBASE ACCELERATOR


## Getting started

1. Install the Databricks CLI from <https://docs.databricks.com/dev-tools/cli/databricks-cli.html>

2. Authenticate to your Databricks workspace, if you have not done so already:

   #### Option A: Personal Access Token (PAT)

   **Generate Personal Access Token:**
      - Log into your Databricks workspace
      - Click on your username in the top-right corner
      - SELECT **User Settings** → **Developer** → **Access tokens**
      - Click **Generate new token**
      - Give it a name (e.g., "Local Development") and set expiration
      - Copy the generated token

   **Configure CLI with PAT:**

   ```bash
   databricks configure --token --profile DEFAULT
   ```

   You'll be prompted for:
   - **Databricks Host**: `https://your-workspace.cloud.databricks.com`
   - **Token**: Paste your generated token

   This will update DEFAULT profile in `~/.databrickscfg`

   #### Option B: OAuth Authentication

   Configure OAuth:

   ```bash
   databricks auth login --host https://your-workspace.cloud.databricks.com --profile DEFAULT
   ```

   This will:

   - Open your browser for authentication
   - Create a profile in `~/.databrickscfg`
   - Store OAuth credentials securely

   #### Verify Databricks Auth

   ```
   databricks auth profiles
   ```

3. To deploy a development copy of this project, type:

    ```
    databricks bundle deploy --target dev
    ```

    (Note that "dev" is the default target, so the `--target` parameter
    is optional here.)

    This deploys everything that's defined for this project.
    For example, the default template would deploy a job called
    `[dev yourname] lakebase_accelerator_job` to your workspace.
    You can find that job by opening your workpace and clicking on **Workflows**.

4. Similarly, to deploy a production copy, type:

   ```
   databricks bundle deploy --target prod
   ```

   Note that the default job from the template has a schedule that runs every day
   (defined in resources/lakebase_accelerator.job.yml). The schedule
   is paused when deploying in development mode (see
   <https://docs.databricks.com/dev-tools/bundles/deployment-modes.html>).

5. To run a job or pipeline, use the "run" command:

   ```
   databricks bundle run
   ```
