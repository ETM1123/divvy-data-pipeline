# Tutorial 

## Introduction  
In our project, we aim to create an interconnected data pipeline that involves multiple technologies. This tutorial section will guide you through the necessary steps to replicate our environment settings for your own personal use or to execute the code in this repository. Our primary technologies include Terraform, GCP, Airflow, Docker, Python, and DBT cloud. We assume that you have a basic understanding of Python and know how to use the command line. Additionally, ensure that you use Python version 3.7 or higher. While we won't cover a lot of the Python code in this tutorial, we have included a readme file in the same directory as the files for your reference. It's worth noting that the configuration files may differ depending on the operating system you are using. Therefore, please refer to the comments provided in this section, which distinguish the setup process based on the operating system you are using.

Furthermore, we offer an array of additional resources to aid in the comprehension of this material. Our primary recommendation for all interested users is to utilize the [Data Talk Club Repository](https://github.com/DataTalksClub/data-engineering-zoomcamp), which provides comprehensive explanations of all the technologies implemented in this project. Additionally, they offer a self-paced Data Engineering course, free of charge, which we strongly recommend to anyone seeking to delve deeper into this subject matter.

In conclusion, we welcome feedback on our repository and encourage you to connect with us if you have any inquiries, require further assistance, or simply wish to engage with like-minded individuals. We believe in fostering a collaborative environment where we can learn, develop, and grow together.

## Step 0: Set up GCP Environment  
In this section, we cover how to setup our GCP environment using Terraform.

- **Sign up**
  - If you don't already have a Google account, create one. Then, visit [this page](https://cloud.google.com/) and authorize access to the Google Cloud Platform.
  - **Note** that all Google users are eligible for a free $300 credit for three months of usage on their cloud services, but a credit card is required to sign up.
- **Create a new project**
  - Steps:
    1. Go to the [Google Cloud Console](https://console.cloud.google.com/) and sign into your account.
    2. In the top navigation bar, select the project drop-down list and click on the "New Project" button.
     3. Enter a unique name for your project and select the billing account that you want to associate with the project.
    4. Select a location for your project. This location determines the default region for your resources. You can change this later if needed.
    5. Click on the "Create" button to create your project.
  - Notes:
    -  Make sure to keep track of your **projectID** and the **location** for your project
    - You can use an existing project but we strongly recommend for  everyone to create a new project. If you decide to use an existing project make sure your project has the same exact setup/authorization as us.
    - Once you have created your project, you can access it from the Google Cloud Console by clicking navigational bar and selecting your project. From now on, we are assuming your project is selected.
 - **Create a service account**
    - Steps:
      1. Go to GCP main page
      2. In the left-hand menu, click on "IAM & Admin" and then select "Service accounts".
      3. Click on the "Create Service Account" button at the top of the page.
      4. Enter a name for the service account and an optional description.
      5. Select the Viewer role and click continue and done. 
   - Notes:
      - A service account is a special type of account that is used to authenticate applications and services to access GCP resources programmatically. 
      - In GCP A Viewer role grants read-only access to specific GCP resources.
- **Authenticate local environment to cloud**
  - steps:
    1. Go to GCP main page
    2.  In the left navigation menu, click on "IAM & Admin" and then "Service accounts".
    3. Click on the three verticals dots under the action section for the service name you just created. 
    4. Then click Manage keys, Add key, Create new key. Select JSON option and click Create.
    5. Go to your download folder and find the json file. 
    6. Rename the json file to google_credentials.json
        ```bash
          mv ~/Downloads/<xxxxxx.json> ~/Downloads/google_credentials.json
        ```
    7. Create the following path .google/credentials/ in your HOME directory. You can use the command below in a terminal.
          ```bash
         mkdir -p $HOME/.google/credentials/ 
          ```
    8. Move the google_credentials.json file to the directory above
        ```bash
        mv ~/Downloads/google_credentials.json ~/.google/credentials/ 
        ```
    9. Follow the instruction on [here](https://cloud.google.com/sdk/docs/install-sdk) to Download and install GCP SDK on your OS. GCP SDK is a CLI tool that can communicate to GCP services from local environment.
    10. After installation, run the following command to see if you installed GCP SDK correctly:
        ```bash
        gcloud -v
        ``` 
    11. Set the GOOGLE_APPLICATION_CREDENTIALS environment variable to the path of the json file. Use the command bellow:
        ``` bash
        export GOOGLE_APPLICATION_CREDENTIALS="~/.google/credentials/google_credentials.json"
        ```

    12.  Run the command bellow - it will make you sign into your google account and verify the authentication. If all goes well your a google browser will open with the following message displayed: **You are now authenticated with the gcloud CLI!**
            ```bash
            gcloud auth application-default login
            ```


  - Notes:
    - In step 11, We're telling Google Cloud SDK and API client libraries to use the JSON file specified by the path as the default location for authentication credentials. This means that we can access Google Cloud Platform services without having to explicitly provide authentication keys in our code. Thus, that's what allows us to automatically sign in into our google account wth the command in step 12. 
    - We changed the name and location of our credentials file for future purpose but ideally we could ran the export command from step 11 when our credential file was located in the downloads folder with its default name. We just had to provide the correct path. 

- **Add additional Roles and permission (i.e services)**
  - steps:
    1. Go to GCP main page
    2. In the left-hand menu, click on "IAM & Admin" and then select "IAM". You should see the service account we created in the previous steps and our Viewer role.
    3. On the same row we see our Service account name, click 'edit principal' button which is located in the last column.
    4. Then add the BigQuery Admin, Storage Admin, Storage Object Admin as roles for our service account and click the save button. 
    5. Enable IAM APIs by clicking the following links:
        - [IAM-API](https://console.cloud.google.com/apis/library/iam.googleapis.com)
        - [IAM-credential-API](https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com)

  - Notes:
    - In Google Cloud Platform (GCP), the Identity and Access Management (IAM) API and the IAM Credentials API are used to manage and control access to GCP resources.
    - The IAM API provides the ability to manage access control policies for GCP resources such as projects, buckets, and instances. It enables you to create and manage IAM roles, which define a set of permissions for specific actions on resources, and grant those roles to members or groups.
    - The IAM Credentials API is used to create, manage, and exchange temporary credentials such as access tokens, identity tokens, and service account keys. These temporary credentials can be used to authenticate requests to GCP APIs and services.
    - Enabling the IAM API and the IAM Credentials API is required in order to use and manage IAM roles and policies, as well as to create and manage temporary credentials.
    - These APIs are required for Terraform (SEE IN NEXT SECTION)


-  Download Terraform 
    - Download Terraform [here](https://www.terraform.io/)
    - Notes:
      - Terraform is an open-source infrastructure-as-code tool developed by HashiCorp. It allows you to define and manage infrastructure resources declaratively, using a high-level configuration language. With Terraform, you can create, modify, and destroy infrastructure resources across various cloud providers, on-premises data centers, and other third-party services.
     - We can use Terraform to bypass the GCP GUI and create GCP infrastructure resources with Terraform configuration code. In our case, we will create a bucket using terraform.

- **Setup Terraform folder**
  - steps:
    1. Create a folder called terraform
      ```bash
      mkdir terraform
      ```
    2. Create the following three files
      ```bash
      touch .terraform-version main.tf variables.tf
      ``` 
- **Add terraform configurations** 

  - Our main Terraform file defines a few infrastructure resources on Google Cloud Platform (GCP). Below is a breakdown of the different blocks:
    - **terraform block**:
      ```terraform
        terraform {
          required_version = ">= 1.0"
          backend "local" {}  
          required_providers {
            google = {
              source  = "hashicorp/google"
            }
          }
        }

      ```

      -  This block defines the required version of Terraform and providers for the deployment. It specifies the minimum required version of Terraform as 1.0 and the provider required for the deployment as google with its source as hashicorp/google. Also, the backend block is defined to store the Terraform state file locally. 

    - **provider block**: 
        ```
        provider "google" {
          project = var.project
          region = var.region
        }
        ```
        - This block defines the configuration required to authenticate with GCP. It specifies the project and region to be used during deployment, which are provided as input variables via the var keyword. 

    - **resource "google_storage_bucket" "data-lake-bucket" block**: 
        ```
      resource "google_storage_bucket" "data-lake-bucket" {
        name          = "${local.data_lake_bucket}_${var.project}" # Concatenating DL bucket & Project name for unique naming
        location      = var.region

        # Optional settings:
        storage_class = var.storage_class
        uniform_bucket_level_access = true

        versioning {
          enabled     = true
        }

        lifecycle_rule {
          action {
            type = "Delete"
          }
          condition {
            age = 30  // days
          }
        }

        force_destroy = true
      }
        ```
        - This block defines a Google Cloud Storage bucket resource that will be created by Terraform. It sets the bucket name, location, storage class, and enables versioning. It also defines a lifecycle rule that will delete objects that are older than 30 days.

    - **resource "google_bigquery_dataset" "dataset" block**:

      ```
      resource "google_bigquery_dataset" "dataset" {
      dataset_id = var.BQ_DATASET
      project    = var.project
      location   = var.region
      }
        
      ```
        -  This block defines a Google BigQuery dataset resource that will be created by Terraform. It sets the dataset ID, project, and region.



  - **Notes**: 
    - Our main file includes the use of input variables, which are defined outside of the main Terraform configuration file and are passed in during execution. These variables are used to define the project ID, region, storage class, and BigQuery dataset name. The declaration of these variables can be found in the variables.tf file.
    - Make sure your project region corresponds with the region you selected when creating GCP project

- **Execute Terraform code**
  - In Terraform we only need few commands to execute and deploy our code. The order and behavior in which we apply these commands are analogs to git commands. Thus, to initialize and execute our configuration files run the commands below (in the exact order).
    - Initialize 
      ```bash
      terraform init
      ```
      - This will initialize your environment and download plugins to connect GCP.
    - Stage
      ```bash
      terraform plan
      ```
      - Think of this like 'git add' command. This will add your configuration plan to a staging area. Then it will ask you for your GCP projectID and display the configuration plan
    - Deploy
      ```bash
      terraform apply
      ```
      - This command applies your configuration file and executes all of your Terraform code. Then you should see the new resources in GCP. This is analogs to git commit and push

<font color="gree">This concludes the set up of our GCP Environment. Our process of creating infrastructure resources on GCP with Terraform involves setting up IAM roles, credentials, defining the infrastructure as code using Terraform, and then applying those changes to our GCP project.</font>

Once you are done with this project, and if you want to discard the infrastructure you created with terraform run the command below:
  ```bash
  terraform destroy
  ```
  <font color="red">**Note this will permanently delete the GCP infrastructure we created with Terraform - use with cation**</font>


## Step 2: Ingest data with Airflow
In this section, we'll cover how to set up our data orchestration tool Apache Airflow. Apache Airflow is an open-source platform to programmatically author, schedule, and monitor workflows. It allows us to define complex workflows as directed acyclic graphs (DAGs) of tasks and dependencies, and manage their execution in a highly scalable and distributed environment.
- Resources:
  - [airflow-docs](https://airflow.apache.org/docs/)
  - [docker-crash-course](https://www.youtube.com/watch?v=3c-iBn73dDE&t=2s)
  - [dtc-airflow-video](https://www.youtube.com/watch?v=lqDMzReAtrw&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=19)
  
- Set up airflow environment
  - steps:
    1. Set up the Airflow environment by creating a new directory and changing your working directory to that new directory

        ```bash
         mkdir airflow
         cd airflow
        ```
    2. Create a new directory for your DAGs:

        ```bash
        mkdir dags
        ```
    3. Create the .env, requirements.txt, and Docker files:

        ```bash
        touch .env requirements.txt Docker
        ```
    4. Download the Docker Compose YAML file for the latest Airflow version:
        ```bash
        curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.2.3/docker-compose.yaml'
        ```
       - This command creates a `docker-compose.yaml` file in your current directory, which is the `airflow `directory.

    5. If you are using a macOS, edit your .env file with `AIRFLOW_UID=50000`. If you are using a different OS, run the following command:
        ```bash
        echo -e "AIRFLOW_UID=$(id -u)" > .env
        ```
    6. Update the requirements.txt file to include `apache-airflow-providers-google` and `pyarrow`, each on its own line.
    7. Update the `.env` file to declare and assign all of the configuration variables to their respective values. You should have the following variables declared:

        ```
        AIRFLOW_USERNAME=<ENTER AIRFLOW USERNAME CREDENTIALS>
        AIRFLOW_PASSWORD=<ENTER AIRFLOW PASSWORD CREDENTIAL>
        GCP_PROJECT_ID=<ENTER GCP PROJECTID CREDENTIALS>
        GCP_GCS_BUCKET=<ENTER GCP BUCKET NAME CREDENTIAL>
        ```
    8. Update the Dockerfile. The Dockerfile is used to build a Docker image for Apache Airflow with the required libraries and settings so that GCP can connect to the image. Here is an explanation of the Dockerfile:
      - First two lines
        ```Dockerfile
        FROM apache/airflow:2.2.3
        ENV AIRFLOW_HOME=/opt/airflow
        ```
          - This line specifies the base image to use - which comes with Apache Airflow 2.2.3 installed, along with common dependencies.
          -  The second line sets the `AIRFLOW_HOME` environment variable to `/opt/airflow`
      - Next three lines
          ```Dockerfile
          USER root
          RUN apt-get update -qq && apt-get install vim -qqq
          RUN apt-get install unzip
          ```
          - These lines switches the user to root then installs vim and unzip packages using apt-get.
            - Note in our dags, we are going to take advantage of some bash operations. Remember to download the necessarily packages, our image does not have the packages and bash operations as our main system. 
      - Next three lines
        ```Dockerfile
        COPY requirements.txt .
        RUN pip install --no-cache-dir -r requirements.txt
        RUN pip install --no-cache-dir pandas sqlalchemy requests psycopg2-binary
        ```
          - These lines copies requirements.txt to the container then uses the pip command to install the requirements.txt file and additional libraries 
      - Next line
        ```Dockerfile
        SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]
        ```
         - This line specifies the shell to be used in subsequent commands, with several options specified (pipefail, e, u, x, and c).
      - Next few lines
    
        ```Dockerfile
        ARG CLOUD_SDK_VERSION=322.0.0
        ENV GCLOUD_HOME=/home/google-cloud-sdk

        ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

        RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
            && TMP_DIR="$(mktemp -d)" \
            && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
            && mkdir -p "${GCLOUD_HOME}" \
            && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
            && "${GCLOUD_HOME}/install.sh" \
              --bash-completion=false \
              --path-update=false \
              --usage-reporting=false \
              --quiet \
            && rm -rf "${TMP_DIR}" \
            && gcloud --version
          ```
          - Thee first line sets CLOUD_SDK_VERSION argument to 322.0.0. Then, we define two environment variables: 
            - GCLOUD_HOME: set to /home/google-cloud-sdk
            - PATH variable: set to include the bin directory in GCLOUD_HOME.
        - Then we use curl to download and extract the Google Cloud SDK from the specified URL, and then run the install.sh script to install the SDK. Several options are specified to disable usage reporting and path updates, and to enable quiet mode and disable bash completion. Finally, the temporary directory used for the download is deleted, and the gcloud command is run to verify the installation.

     - Last two lines
        ```Dockerfile
        WORKDIR $AIRFLOW_HOME
        USER $AIRFLOW_UID
        ```
          - The WORKDIR command sets the working directory to $AIRFLOW_HOME.
          - USER command sets the user to $AIRFLOW_UID, which is not defined in this Dockerfile but is in the .env file 

  9. Update the default docker-compose.yaml file to point to our Dockerfile and environment variables. The default docker-compose file deploys multiple containers and requires a lot of resources to run. For our purpose we only need three services, postgres, airflow-webserver, and airflow-scheduler. The default docker-compose file is couple of 100 lines long, w're only going to show and explain the main changes.
      - Connect the docker-compose file with our  Dockerfile. In the build section add the following:
        ```yaml
        build:
          context: .
          dockerfile: ./Dockerfile
        ```
      - In the volume section, add the line below - that
      mounts the `~/.google/credentials` directory to the container, allowing the container to access our google credential json file from the host machine.
      ```yaml
      Volumes:
        - ~/.google/credentials/:/.google/credentials:ro
      ```
      - In the environment section change the AIRFLOW__CORE__EXECUTOR variable from CeleryExecutor to LocalExecutor and and remove AIRFLOW__CELERY__RESULT_BACKEND and AIRFLOW__CELERY__BROKER_URL variables. The enviroment section should look like the following:
        ```yaml
        environment:
          &airflow-common-env
          AIRFLOW__CORE__EXECUTOR: LocalExecutor
          AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
          AIRFLOW__CORE__FERNET_KEY: ''
          AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
          AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
          AIRFLOW__API__AUTH_BACKEND: 'airflow.api.auth.backend.basic_auth'
          _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:-}
        ```
      - Add the following 4 variables at the end of the environment section.
          ```yaml
          GOOGLE_APPLICATION_CREDENTIALS: /.google/credentials/google_credentials.json
          AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT: 'google-cloud-platform://?extra__google_cloud_platform__key_path=/.google/credentials/google_credentials.json'
          GCP_PROJECT_ID: '${GCP_PROJECT_ID}'
          GCP_GCS_BUCKET: "${GCP_GCS_BUCKET}"
          ```

     - Remove redis, airflow-worker, airflow-triggerer and flower services. At the end of x-airflow-common, remove the redis dependency. 
     - In the environment section in the air-flow-init service, remove the hard coded airflow credentials and use the vairble name from the .env file. This section should look like the following:
        ```yaml
        environment:
          <<: *airflow-common-env
          _AIRFLOW_DB_UPGRADE: 'true'
          _AIRFLOW_WWW_USER_CREATE: 'true'
          _AIRFLOW_WWW_USER_USERNAME: ${AIRFLOW_USERNAME}
          _AIRFLOW_WWW_USER_PASSWORD: ${AIRFLOW_PASSWORD}
        ```

- Start airflow and test configurations:
  - Run the following command to start Airflow:
    ```bash
    docker-compose build
    ```
      - Note: in my case I renamed my docker-compose.yaml file to docker-compose-gcp.yaml You can run a named docker-compose file by the following command:
        ```bash
        docker-compose -f docker-compose-gcp.yaml build 
        ``` 
      - First time building our files should take about 10 to 12 minutes since we have to download all of the dependencies to our image. 

  - Initialize our files by running the following command:
      ```bash
      docker-compose up airflow-init
      ```
  - To run airflow servers, run the following command:

    ```
    docker-compose up
    ```
    - This command will start the Airflow web server, scheduler, and worker services. If everything is configured correctly, you should see similar messages in your terminal like the ones below:
      ```css
      Starting airflow_airflow-init_1 ... done
      Starting airflow_airflow-webserver_1 ... done
      Starting airflow_airflow-scheduler_1 ... done 
      ```
  - To stop Airflow, press `Ctrl+C` in the terminal window where you ran `docker-compose up` then run the following:
      ```bash
      docker-compose down
      ```
  -  Verify there's no containers running by running the following command:
      ```
        docker ps 
      ```
        - This will list all of the active containers. If you ran docker-compose down, then the docker ps command should return nothing. If the containers didn't shut down correctly with the docker-compose file you can copy the container id then run the following:
          ```
          docker kill container_id
          ```
- Dags
  - In our project, we aim to develop Airflow DAGs that perform the following tasks:
    - Downloading Divvy's bike trip data, which is available on the web in the form of zip files.
    - Extracting the relevant data from the downloaded zip files.
    - Uploading the extracted data to Google Cloud Storage (GCS) in the form of Parquet files.
    - Formatting the data in a suitable manner and transferring it to Google BigQuery (GBQ).
  - To achieve the above tasks, we can create two DAGs: `ingest_data_to_gcs` and `gcs_to_gbq`. The `ingest_data_to_gcs` DAG includes the following tasks in sequence: 
   $$  \text{download dataset} \rightarrow \text{unzip dataset} \rightarrow \text{format to parquet} \rightarrow \text{upload to gcs} $$
  - Once all the data has been successfully uploaded to GCS, we can activate the `gcs_to_gbq` DAG, which involves creating an external table and a partitioned table in GBQ to store the formatted data.
    $$\text{create external table} \rightarrow \text{create partitioned table}$$

  - To execute the `create partitioned table` task in the `gcs_to_gbq` dag, we have to create a bigquery connection with Airflow. We can create bigquery connection by following the steps below:
    - Steps to creating bigquery connection:
      - Boot up the airflow server
      - Login into the airflow UI
      - click on the "Admin" menu.
      - Select "Connections" from the drop-down menu
      - Click on the "Create" button to add a new connection
      - In the "Connection Type" drop-down menu, select "Google Cloud Platform".
      - In the "Connection ID" field, enter a name for the connection (e.g., "my_bigquery_connection").
      - In the "Extra" field, enter the following JSON with your own credentials:
          ```json
          {
          "project_id": "<YOUR_PROJECT_ID>",
          "keyfile_path": "<PATH TO GOOGLE CREDENTIAL ON AIRFLOW>"
          }
          ```
          - The path to your google credentials on the airflow is the following `/.google/credentials/google_credentials.json`.
            - To verify this file exist in airflow run the following command in a new terminal session: `docker ps`
        
            - This should list the active services that is currently running. Copy the container_id for airflow-webserver service then run:
              ```bash
              docker exec container_id -it bash
              ```
            - The code above should open your airflow bash. Then run
              ```bash
              ls /.google/credentials 
              ```
            - If this lists a json file named google_credentials then you are good to set the path to this json file for `keyfile_path`. 
          - In the bottom left corner click the test connection button - make sure your connection passes then click save connection button. 

 
  - Execute dags
    - steps:
      1. Boot up airflow server
      2. Airflow's GUI should be available - log into your account using your credentials
      3. In the airflow's GUI, you should see your dag, turn it on then trigger it to run. 
      4. Turn off dag if there's no future use 
      5. Check GCS to see if data was stored correctly
      6. Once the execution is done, shut down the airflow server
          ```bash
          docker-compose down
          ``` 
  - As mentioned above, Airflow UI provides a graphical representation of your dags. After execution, Airflow should provide the following graphs:

  **Graph for ingest to GCS dag**
  ![ingest-to-gcs](images/ingest_gcs_dag.png)
  **Graph for ingest to GBQ dag**
  ![ingest-to-gcs](images/ingest_gbq_dag.png)

  
<font color="gree">This concludes our orchestration section with Airflow. We know have all of our data in Bigquery and ready to transform our dataset for production use.</font>

## Step 3: Transform data DBT
In this section, we will utilize DBT Cloud to transform our dataset and generate models that will prepare our data for production. DBT is a Data Building Tool that offers an environment to develop, test, and deploy models. All computations and processes are executed using GCP BigQuery functionality. 
- resources 
  - [dtc-dbt-video](https://www.youtube.com/watch?v=Cs9Od1pcrzM&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=38)
  - [dbt-labs](https://docs.getdbt.com/docs/quickstarts/overview)
- DBT development SET-UP
  - Steps:
    1. If you don't already have a DBT Cloud account, create one at [here](https://cloud.getdbt.com/login/)
    2. Create a new project in DBT Cloud and select "Google BigQuery" as your database type.
    3. Enter the credentials for the service account that you created earlier in this tutorial. You can autofill the credentials section by passing in the JSON file.
    4. Enter the name of the BigQuery dataset that was created in the previous section.
    5. Connect your DBT project to your Git repository. It's fine if you don't have a GitHub account - you can still use DBT Cloud.
    5. Initialize a starter project, which will create the following folders: analysis, logs, macros, models, seeds, snapshots, and tests; as well as the following files: .gitignore and dbt_project.yml.
    6. Create new brach
    8. Update the dbt_project.yml file, changing the name of the project to your liking and adding the following seed section:
        ```yaml
        seeds: 
          divvy_historic_biketrips:               
              divvy_stations_lookup:
                  +column_types:
                      station_id: INTEGER
                      station_name: STRING
                      lat: FLOAT
                      lng: FLOAT
                      state: STRING
                      primary_neighborhood: STRING
                      secondary_neighborhood: STRING
        ```
          - The project name is divvy_historic_biketrips. An additional dataset called stations_lookup will be used. The station_id column in this dataset was inconsistent, so a new unique station id was generated using station names and additional geolocation information. The code for this can be found [here](https://github.com/ETM1123/Divvy-Bikeshare-Project), and the dataset is located in the dbt/seeds section.
    9. Create a new file called packages.yml with the following content:
        ```yaml
        packages:
          - package: dbt-labs/dbt_utils
            version: 0.8.0
        ```
    10. Run the following command at the bottom panel
        ```bash
          dbt deps
        ```
    11. Clean up the files by deleting the examples folder in the models folder.
    12. Add files by creating two additional folders in the models folder:
        - staging
          - schema
          - stg_divvy_tripdata.sql
        - divvy_core
          - dim_neighborhoods.sql
          - facts_divvy_trips.sql
        - Also, create the following file in the seeds folder:
          - divvy_stations_lookup.sql
            - Copy and paste the CSV file from this repository to the file above.
    13. Run the command `dbt seed --full-refresh`.

    14. Populate files in models folder
        - staging/schema.yaml: This file connects to Google BigQuery and generates a lineage for our data models. Add the following:
          ```yaml
          version: 2
          sources:
              - name: staging
                database: <GCP PROJECT ID>
                schema: <NAME OF DATASET IN BQ>
                tables:
                  - name: <NAME OF TABLE IN BQ>

          models:
            - name: stg_divvy_tripdata
          ```
            - The data from BigQuery can now be referenced using Jinja-like code, such as:
              ```sql
                SELECT * 
                FROM {{ source('staging','divvy_tripdata') }}
              ```
            - This YAML file helps generate a lineage for our data models. Additional fields can also be added to schema.yaml to allow for testing of data models during execution. Check the file in the repository.
      
        - The `staging/stg_divvy_tripdata.sql` file is a raw model that cleans and formats the dataset for analysis using SQL code.
        - The `divvy_core/dim_neighborhoods.sql` model converts the CSV file in the seeds folder into a data model that can be used throughout the project.
        - The `divvy_core/facts_trips.sql` model is for production.
    15. To run the model, use the following dbt command:
        ```dbt 
        dbt run --select +facts_divvy_trips.sql
        ```
        - Note that after execution, all of the models will be accessible in the GCP Big Query service. This will run the model and all of its dependencies.
        
    - If all of the settings are correct, dbt should generate the following lineage for the data models:
      ![Linage](images/dbt_model_linage.png)
  16. Once the changes are merged to the main branch, the project is ready to be deployed.

- To deploy a DBT Cloud project, follow these steps:
  1. In GCP BigQuery, create a dataset for production models.
  2. In the DBT Cloud navigator, go to the "Deploy" section and select "Environments."
  3. Click the "Create a new environment" button.
  4. Fill out the form, making sure to select "Production" as the environment type.
  5. Provide the name of the dataset for production models (the one created in step 1) and save the environment.
  6. In the navigator section, go to "Deploy" and select "Jobs."
  7. Click "New Job."
  8. Provide a name for the job and select the production environment created in the previous step.
  9. In the "Command" section, provide the commands you want to execute. If you have multiple models in your cloud repository, you may want to specify the model to run. Otherwise, run these commands: dbt seed, dbt run, dbt test.
  10. In the "Scheduler" section, provide the frequency for the program to deploy.

  After creating a deployment job with dbt cloud, dbt will automatically run your models based on the schedule you provided in the scheduler section. You can use dbt cloud to monitor the status of your models.
  ![dbt](images/dbt_job.png)

  <font color='gree'>This concludes our data transformation stage with DBT cloud. We know have a dataset that's ready for production in our Data Warehouse.</font>

## Step 4: Create Visualizations with Looker
- In this section, we'll show how to connect your dataset with Google Data Studio Looker and create a dashboard. Again, we're assuming you have production ready dataset in BigQuery.
- steps:
  - In Data Studio, create a new report and select BigQuery as your data source. Authenticate and connect your BigQuery project to Data Studio.
  - Create a new dashboard in Data Studio: Add charts, tables, and other visualizations to your dashboard using the data from your BigQuery dataset. Customize the layout and style of your dashboard as desired.
    - You can use the following video to design your dashboard: [dashboard-tutorial](https://www.youtube.com/watch?v=nLHypcQymkY)

We can make powerful dashboards with ease using Google Data Studio. Here's a dashboard that provides an overview of our analysis. 

![Dashboard](images/divvy_overview_dashboard.png)

