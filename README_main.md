

### Prerequisites

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

