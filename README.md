### Airflow DAG Manager

A Python script that dynamically **generates and manages DAGs** in **Apache Airflow** using **MySQL configurations** and **API integrations**.

## 🚀 Features
- **Dynamically generates DAGs** based on database configurations.
- **Fetches task configurations from MySQL** and automates scheduling.
- **Executes API calls** based on DAG parameters.
- **Supports email notifications** for DAG execution status.
- **Logs and handles errors gracefully** for reliable workflow execution.

---

## 🔧 **Installation**

### **1. Install Python and Required Libraries**
Ensure Python is installed. If not, download it from [Python.org](https://www.python.org/downloads/).

Then, install the required dependencies:

```sh
pip3 install pymysql apache-airflow requests
```

### **2. Set Up Apache Airflow**
#### **Step 1: Install Airflow**
```sh
pip3 install apache-airflow
```
#### **Step 2: Initialize the Airflow Database**
```sh
airflow db init
```
#### **Step 3: Start the Airflow Web Server**
```sh
airflow webserver --port 8080
```
#### **Step 4: Start the Airflow Scheduler**
```sh
airflow scheduler
```

---

## 🏗 **MySQL & Airflow Variable Setup**
### **1. Create Airflow Variables**
Add the following variables to **Airflow UI** (`Admin > Variables`):
| Variable Name      | Description                         |
|--------------------|-------------------------------------|
| `DB_HOST`         | MySQL database host address         |
| `DB_NAME`         | MySQL database name                 |
| `DB_USER`         | MySQL username                      |
| `DB_PASSWORD`     | MySQL password                      |
| `API_URL`         | API endpoint for data processing    |
| `BEARER_TOKEN`    | API authentication token           |

### **2. MySQL Table Structure**
Ensure your **MySQL database** has a table with the following schema:
```sql
CREATE TABLE airflow_scheduler (
    id INT PRIMARY KEY AUTO_INCREMENT,
    dag_id VARCHAR(255),
    name VARCHAR(255),
    schedule VARCHAR(255),
    start_date DATETIME,
    owner VARCHAR(255),
    retries INT DEFAULT 1,
    retry_delay INT DEFAULT 5,
    email_on_failure BOOLEAN DEFAULT 0,
    email_on_retry BOOLEAN DEFAULT 0,
    email VARCHAR(255),
    is_paused_c BOOLEAN DEFAULT 0,
    select_command_c TEXT,
    api_method_c VARCHAR(50),
    email_notification_c BOOLEAN DEFAULT 0,
    deleted BOOLEAN DEFAULT 0
);
```

---

## 🎯 **Running the Script**
### **Step 1: Clone the Repository**
```sh
git clone https://github.com/bharatcj/airflow-dag-manager.git
cd airflow-dag-manager
```

### **Step 2: Run the Script**
Place the script inside your Airflow **DAGs folder**:
```sh
cp airflow-dag-manager.py ~/airflow/dags/
```
Then, restart Airflow:
```sh
airflow scheduler
airflow webserver
```

---

## 📜 **Example DAG Execution**
### **Uploading Data via DAG**
```sh
airflow dags trigger my_dynamic_dag
```
**Expected Output:**
```
File uploaded successfully and indexed in Elasticsearch.
```

### **Checking DAG Logs**
```sh
airflow dags list-runs --dag-id my_dynamic_dag
```

---

## ⚠️ **Error Handling**
If an error occurs:
- The script **logs database and API errors** in Airflow logs.
- **Sends email notifications** for failures (if enabled).
- **Retries API requests** in case of intermittent failures.

---

## 🔄 **Customization**
- Modify `airflow-dag-manager.py` to **change the API request structure**.
- Adjust **email notification settings** based on your requirements.

---

## 🛡️ **License**
This project is licensed under the **MIT License**.

---

## 🤝 **Contributing**
Contributions are welcome! Feel free to **fork this repository** and submit pull requests.

---

### **Author**
Developed by **Bharat CJ**  
GitHub: https://github.com/bharatcj

---

💡 **Did you know?** Apache Airflow allows you to schedule complex workflows and monitor execution with just a few lines of code! 🚀