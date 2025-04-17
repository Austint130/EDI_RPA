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
