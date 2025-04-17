# Overview ##

This application provides tools to automate the processing of Electronic Data Interchange (EDI) files. It monitors source folders specific to configured clients, processes valid EDI files based on global rules (Special Instructions), and automatically uploads successfully processed files to client-specific FTP destinations.

It offers two ways to run:
1.  **GUI Version (`edi_processor_combined.py`):** A graphical user interface for interactive use, configuration management, and visual monitoring.
2.  **Background Version (`edi_processor_background.py`):** A command-line script suitable for running as a background process, scheduled task, or service, without a GUI.

## Features ##

* **GUI Interface:** (GUI Version Only) Provides a user-friendly interface for starting/stopping processes, viewing logs, and managing all configurations.
* **Client Profiles:** Manage configurations for multiple clients, including their specific source folders and FTP destinations.
* **Configurable Folders:** Define default and client-specific folders for source, processing, processed, secured, and error files via `config.json` and GUI windows.
* **Special Instructions:** Apply global, rule-based actions (renaming, opening external programs, sending custom email notifications) to files containing specific keywords in their filenames during processing.
* **Automated FTP Upload:** Automatically uploads successfully processed files to the FTP destination configured for the active client (using standard FTP).
* **Scheduling:** (GUI Version) Run processing automatically at configurable intervals with GUI controls. (Background Version) Run processing in a continuous loop with configurable intervals via command-line.
* **Logging:** Outputs detailed logs for monitoring and troubleshooting. (GUI version shows logs in the window; Background version logs to console and `edi_processor_background.log`).
* **Notifications:** Configurable (simulated) email notifications for errors and batch summaries.

## Project Structure ##

* `edi_processor_combined.py`: The main Python script for the **GUI version**. Contains all application logic and GUI code.
* `edi_processor_background.py`: A separate Python script for the **Background/Command-Line version**. Contains only the processing logic, configured via     `config.json` and command-line arguments.
* `config.json`: **Crucial file.** Stores all configuration settings used by *both* versions (general paths, client profiles, active client, special instructions, notification settings). It is recommended to edit this file primarily through the GUI version (`edi_processor_combined.py`).
* `schedule_state.json`: (Used by GUI Version) Stores whether the scheduler was running when the GUI application last closed (used for auto-restart).
* `edi_processor_background.log`: (Used by Background Version) Log file where the background script writes its operational messages and errors.
* `/source_folder/` (Example): Default or client-specific folder where incoming EDI files are placed. The actual path(s) are defined in `config.json`.
* `/processing_folder/`: Temporary folder used during file processing.
* `/processed_folder/`: Folder where files are moved after successful parsing but before FTP/Securing.
* `/secured_folder/`: Final destination for successfully processed and uploaded files.
* `/error_folder/`: Folder where files are moved if an error occurs during processing.
* `README.md` (This file)


## Prerequisites

* Python 3.x installed on the system.
* Access permissions for the configured source, processing, processed, secured, and error folders.


### Option 1: GUI Version ##

* **Purpose:** Interactive use, configuration management, visual monitoring.
* **Command:**
    ```bash
    python edi_final.py
    ```
* **Usage:**
    1.  Use the "**Clients**" button to add/edit clients and **Set Active Client**.
    2.  Use "**Settings**", "**Notifications**", "**Instructions**" to configure details.
    3.  Place files in the **Active Client's Source Folder**.
    4.  Click "**Manual Run**" or configure schedule settings and click "**Start Schedule**".
    5.  Monitor the log area.

### Option 2: Background Version

* **Purpose:** Automated processing without user interaction (e.g., run via Windows Task Scheduler). Reads configuration from `config.json`.
* **Command:** Requires specifying the client to run for.
    * **Run continuously (looping):**
        ```bash
        # Process for ClientA, check every 5 minutes (uses interval from config or default)
        python edi_processor_background.py --client ClientA --mode loop

        # Process for ClientB, check every 10 minutes (overrides config interval)
        python edi_processor_background.py --client ClientB --mode loop --interval 10

        # Process for client with spaces in name
        python edi_processor_background.py --client "Internal Process" --mode loop
        ```
        *Press `Ctrl+C` to stop the loop.*
    * **Run only once:**
        ```bash
        python edi_background.py --client ClientA --mode single
        ```
* **Usage:**
    1.  Ensure `config.json` is correctly configured (use the GUI version for this).
    2.  Run the script from the command line using the desired arguments (`--client` is required).
    3.  Monitor activity and errors by checking the console output and the `edi_processor_background.log` file.


4.  **Activate Virtual Environment:** (Run *every time* you open a new terminal for this project)#This needs to be done only once when setting up for both the gui and the background script, once its done its all set. 


1.  **Open Terminal:** Open PowerShell or Command Prompt.
2.  **Navigate to Project Directory:**
    ```bash
    cd C:\Users\Austin\Desktop\EDI_process
    ```
3.  **Create Virtual Environment:** (Run this once)
    ```bash
    python -m venv venv
    ```
    * PowerShell: `.\venv\Scripts\Activate.ps1`
    * Command Prompt: `venv\Scripts\activate.bat`
    *(Your prompt should now show `(venv)` at the beginning)*.
5.  **Install Dependencies:** Currently uses built-in libraries. If SFTP support (e.g., via `paramiko`) is added later, install it here while the venv is active: `pip install paramiko`.
6.  **Deactivate (Optional):** Type `deactivate` when done.

**TROUBLESHOOTING**

* **Files Not Detected:**
    * (GUI): Is the correct client set as **Active**?
    * (Background): Did you specify the correct `--client` name?
    * Is the **Source Folder** path in the active client's profile correct and accessible? Does the directory exist? (The script tries to create it, but might fail due to permissions).
    * Do files have the correct **EDI Extension** (check `config.json`)?
    * Do filenames contain any **Ignore Keywords** (check `config.json`)?
    * Check logs (GUI or `edi_processor_background.log`) for errors accessing the source folder.
* **FTP Uploads Fail:**
    * Verify FTP Host, Port, User, Password, Remote Path in the active client's profile.
    * Check network connectivity and FTP server status/logs.
    * Check user permissions on the FTP server.
* **GUI Windows Don't Open:** Ensure you are running `edi_processor_combined.py` and all necessary code is within that single file.
* **Check Logs:** Logs are crucial for diagnosing issues in both versions.



