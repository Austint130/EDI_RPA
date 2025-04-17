#This needs to be done only once when setting up for both the gui and the background script, once its done its all set. 


1.  **Open Terminal:** Open PowerShell or Command Prompt.
2.  **Navigate to Project Directory:**
    ```bash
    cd C:\Users\Austin\Desktop\EDI_process
    ```
3.  **Create Virtual Environment:** (Run this once)
    ```bash
    python -m venv venv
    ```
4.  **Activate Virtual Environment:** (Run *every time* you open a new terminal for this project)
    * PowerShell: `.\venv\Scripts\Activate.ps1`
    * Command Prompt: `venv\Scripts\activate.bat`
    *(Your prompt should now show `(venv)` at the beginning)*.
5.  **Install Dependencies:** Currently uses built-in libraries. If SFTP support (e.g., via `paramiko`) is added later, install it here while the venv is active: `pip install paramiko`.
6.  **Deactivate (Optional):** Type `deactivate` when done.

