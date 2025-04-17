## Overview ##

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
