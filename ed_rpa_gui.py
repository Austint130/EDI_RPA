#!/usr/bin/env python3
import logging
import os
import shutil
import sys
import time
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, simpledialog, filedialog
import datetime
import threading
import subprocess
import json
import traceback
import ftplib  # For FTP uploads
import socket  # For FTP error handling
from typing import Any, Dict, Optional, Union

# Potentially problematic imports - comment out if not available/needed
# from _interpreters import is_running
# from Scripts.ParseEDI import file_path # Example, if you have custom parsing logic

# ==========================================================
# Configuration Defaults and Handling
# ==========================================================
CONFIG_FILE = 'config.json'
STATE_FILE = "schedule_state.json" # File to store schedule running state

# Define default structures and values for configuration
DEFAULT_NOTIFICATION_SETTINGS = {
    "recipient_email": "",
    "send_on_error": True,
    "send_on_success_summary": False,
    "include_original_filename": True,
    "include_new_filename": True,
    "include_timestamp": True,
    "include_error_details": True,
    "include_success_fail_counts": True,
    "subject_error": "EDI Process Error",
    "subject_summary": "EDI Process Batch Summary"
}
DEFAULT_CLIENT_PROFILE = {
    "source_folder": "",
    "processing_folder": "",
    "processed_folder": "",
    "secured_folder": "",
    "error_folder": "",
    "ftp_host": "",
    "ftp_port": 21,
    "ftp_user": "",
    "ftp_password": "",
    "ftp_remote_path": "/"
}
DEFAULT_CONFIG = {
    "source_folder": "",
    "processing_folder": "",
    "processed_folder": "",
    "secured_folder": "",
    "error_folder": "",
    "filezilla_exe_path": "", # Example: Path to an external program if needed
    "ignore_keywords": ["log", "temp", "tmp", "ignore"], # Files containing these (case-insensitive) are skipped
    "edi_extension": ".edi", # Expected file extension for EDI files
    "schedule_interval_minutes": 5, # Default schedule check interval
    "auto_restart_schedule": True, # Whether to restart schedule automatically on app launch if it was running
    "special_instructions": [], # List of special rules for files
    "notification_settings": DEFAULT_NOTIFICATION_SETTINGS.copy(), # Default notification settings
    "active_client": None, # Name of the currently active client profile
    "clients": {} # Dictionary to hold client profiles
}
DEFAULT_SPECIAL_INSTRUCTION = {
    "enabled": False,
    "contains": "",         # Keyword in filename to trigger rule
    "new_filename": "",     # Base for new filename (timestamp/counter added)
    "open_exe": "",         # Path to an executable to run
    "send_email": False,    # Send a specific email for files matching this rule?
    "email_subject": "",    # Subject for the rule-specific email
    "email_body": ""        # Body template for the rule-specific email ({orig}, {new} placeholders)
}

def load_configuration() -> Dict:
    """Loads configuration from JSON file, applying defaults for missing keys."""
    config = DEFAULT_CONFIG.copy() # Start with hardcoded defaults
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                loaded_config = json.load(f)

            # Merge loaded config over defaults (top level)
            config.update(loaded_config)

            # --- Deep Merge/Validation for Nested Structures ---

            # Notification Settings: Ensure all keys exist, using defaults if missing
            loaded_notif = config.get('notification_settings', {})
            if not isinstance(loaded_notif, dict): loaded_notif = {} # Handle invalid type
            config['notification_settings'] = {**DEFAULT_NOTIFICATION_SETTINGS, **loaded_notif}

            # Clients: Ensure it exists and profiles have all keys
            loaded_clients = config.get('clients', {})
            if not isinstance(loaded_clients, dict): loaded_clients = {} # Handle invalid type
            config['clients'] = {} # Reset and rebuild validated client dict
            for name, profile in loaded_clients.items():
                 if isinstance(profile, dict):
                      config['clients'][name] = {**DEFAULT_CLIENT_PROFILE, **profile}

            # Active Client: Ensure it's a string or None
            if not isinstance(config.get('active_client'), (str, type(None))):
                 config['active_client'] = None

            # Special Instructions: Ensure it's a list and items are valid dicts
            loaded_instr = config.get('special_instructions', [])
            if not isinstance(loaded_instr, list): loaded_instr = [] # Handle invalid type
            config['special_instructions'] = [] # Reset and rebuild validated list
            for instr in loaded_instr:
                if isinstance(instr, dict):
                     config['special_instructions'].append({**DEFAULT_SPECIAL_INSTRUCTION, **instr})

            print("INFO: Configuration loaded successfully.")

        except FileNotFoundError:
            # Should not happen due to os.path.exists, but good practice
            print(f"INFO: {CONFIG_FILE} not found during read. Using defaults.")
        except json.JSONDecodeError as e:
            print(f"ERROR loading config: Invalid JSON in {CONFIG_FILE}: {e}. Using defaults.")
            messagebox.showerror("Config Error", f"Invalid JSON in {CONFIG_FILE}:\n{e}\n\nUsing default settings.")
            config = DEFAULT_CONFIG.copy() # Reset to defaults on JSON error
        except Exception as e:
            print(f"ERROR loading config: An unexpected error occurred: {e}. Using defaults.")
            messagebox.showerror("Config Error", f"Unexpected Load Error: {e}\n\nUsing default settings.")
            config = DEFAULT_CONFIG.copy() # Reset to defaults on other errors
    else:
        print(f"INFO: {CONFIG_FILE} not found. Creating default configuration file.")
        # Save the default config if the file doesn't exist
        save_configuration(config) # Call save to create the file

    return config


def save_configuration(config_data: Dict) -> bool:
    """Saves the provided configuration data dictionary to the JSON file."""
    try:
        # --- Pre-Save Validation/Cleanup (Optional but recommended) ---
        # Ensure top-level keys exist
        for key, default_value in DEFAULT_CONFIG.items():
            config_data.setdefault(key, default_value)

        # Ensure nested structures are valid before saving
        config_data['notification_settings'] = {**DEFAULT_NOTIFICATION_SETTINGS, **config_data.get('notification_settings', {})}

        validated_clients = {}
        for name, profile in config_data.get('clients', {}).items():
             if isinstance(profile, dict):
                  validated_clients[name] = {**DEFAULT_CLIENT_PROFILE, **profile}
        config_data['clients'] = validated_clients

        validated_instructions = []
        for instr in config_data.get('special_instructions', []):
             if isinstance(instr, dict):
                  validated_instructions.append({**DEFAULT_SPECIAL_INSTRUCTION, **instr})
        config_data['special_instructions'] = validated_instructions

        # --- Save to File ---
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config_data, f, indent=4) # Use indent for readability

        print(f"INFO: Configuration successfully saved to {CONFIG_FILE}")
        return True # Indicate success

    except Exception as e:
        print(f"ERROR saving configuration to {CONFIG_FILE}: {e}")
        messagebox.showerror("Save Error", f"Failed to save configuration:\n{e}")
        return False # Indicate failure

# ----------------------------------------------------------

def parse_file(file_path: str) -> bool:
    """
    Placeholder function to simulate parsing an EDI file.
    Checks for the word 'fail' (case-insensitive) to simulate processing errors.

    Args:
        file_path: The path to the file to parse.

    Returns:
        True if parsing is simulated successfully.

    Raises:
        ValueError: If the file content contains 'fail' or if other errors occur.
    """
    try:
        # Check if file exists before trying to open
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found at path: {file_path}")

        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f: # Specify encoding, ignore errors
            contents = f.read()

        # Simulate failure condition
        if "fail" in contents.lower():
            raise ValueError("Simulated failure: Found 'fail' keyword in file content.")

        # Simulate some processing time (optional)
        # time.sleep(0.05)

        return True # Indicate success

    except FileNotFoundError as fnf_err:
         # Re-raise FileNotFoundError with a more specific message if needed
         raise ValueError(f"Parsing failed: {fnf_err}") from fnf_err
    except Exception as e:
        # Catch other potential errors (permissions, specific encoding issues not ignored, etc.)
        raise ValueError(f"Parsing failed for '{os.path.basename(file_path)}': {e}") from e


# ==========================================================
# Helper Function for File Browsing (Used by multiple windows)
# ==========================================================
def _browse_file_dialog(entry_widget: tk.Entry, parent_window: tk.Toplevel):
    """Opens a file dialog to select a file and updates the entry widget."""
    initial_dir = "/" # Default initial directory
    current_path = entry_widget.get()
    # Try to set initial directory based on current entry value
    if current_path and os.path.exists(os.path.dirname(current_path)):
        initial_dir = os.path.dirname(current_path)

    # Open file dialog
    file_path = filedialog.askopenfilename(
        initialdir=initial_dir,
        parent=parent_window # Ensure dialog is modal to its parent
    )
    # Update entry widget if a file was selected
    if file_path:
        entry_widget.delete(0, tk.END)
        entry_widget.insert(0, file_path)

# ==========================================================
# General Settings GUI Window Class
# ==========================================================
class GeneralConfigWindow(tk.Toplevel):
    """GUI window for editing general application settings."""
    def __init__(self, master, current_config: Dict, save_callback: callable):
        super().__init__(master)
        self.title("General Settings")
        self.geometry("700x350")
        self.transient(master) # Keep on top of master window
        self.grab_set() # Make window modal

        # Keys managed by this window
        self.GENERAL_CONFIG_KEYS = [
            "source_folder", "processing_folder", "processed_folder",
            "secured_folder", "error_folder", "filezilla_exe_path",
            "ignore_keywords", "edi_extension"
        ]

        self.config_vars = {} # To hold tk.StringVar for entries
        self.current_config = current_config # Reference to the main config dict
        self.save_callback = save_callback # Function to call for saving

        # --- GUI Layout ---
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        row_counter = 0
        for key in self.GENERAL_CONFIG_KEYS:
            # Label for the setting
            label_text = key.replace('_', ' ').title() + ":"
            ttk.Label(main_frame, text=label_text).grid(row=row_counter, column=0, padx=5, pady=4, sticky=tk.W)

            # Get current value, handle list case for ignore_keywords
            current_value = self.current_config.get(key, "")
            if key == "ignore_keywords" and isinstance(current_value, list):
                current_value = ", ".join(current_value) # Display list as comma-separated string

            # Create StringVar and Entry widget
            var = tk.StringVar(value=current_value)
            self.config_vars[key] = var
            entry = ttk.Entry(main_frame, textvariable=var, width=70)
            entry.grid(row=row_counter, column=1, padx=5, pady=4, sticky=tk.EW)

            # Add browse button for folders or file paths
            browse_cmd = None
            button_text = ""
            if "folder" in key:
                # Pass the specific entry widget to the lambda
                browse_cmd = lambda e=entry: self._browse_folder(e)
                button_text = "..."
            elif "path" in key: # e.g., filezilla_exe_path
                # Use the shared file browser function
                browse_cmd = lambda e=entry: _browse_file_dialog(e, self)
                button_text = "..."

            if browse_cmd:
                # Create browse button if applicable
                ttk.Button(main_frame, text=button_text, command=browse_cmd, width=3).grid(row=row_counter, column=2, padx=5, pady=4)

            row_counter += 1

        # Configure column 1 (holding entries) to expand horizontally
        main_frame.columnconfigure(1, weight=1)

        # --- Save/Cancel Buttons ---
        button_frame = ttk.Frame(main_frame)
        # Place below the setting entries
        button_frame.grid(row=row_counter, column=0, columnspan=3, pady=(15, 0))
        ttk.Button(button_frame, text="Save", command=self._save_settings).pack(side=tk.RIGHT, padx=5)
        ttk.Button(button_frame, text="Cancel", command=self.destroy).pack(side=tk.RIGHT)

    def _browse_folder(self, entry_widget: tk.Entry):
        """Opens a directory dialog and updates the associated entry widget."""
        initial = entry_widget.get()
        # Set initial directory for dialog, default to root if invalid
        initialdir = initial if os.path.isdir(initial) else "/"
        folder = filedialog.askdirectory(initialdir=initialdir, parent=self)
        # Update entry if a folder was selected
        if folder:
            entry_widget.delete(0, tk.END)
            entry_widget.insert(0, folder)

    def _save_settings(self):
        """Updates the main config dictionary and calls the save callback."""
        print("DEBUG: GeneralConfigWindow._save_settings called")
        try:
            # Update the main config dict from the tk variables
            for key in self.GENERAL_CONFIG_KEYS:
                if key in self.config_vars:
                    value = self.config_vars[key].get()
                    if key == "ignore_keywords":
                        # Convert comma-separated string back to list of strings
                        self.current_config[key] = [item.strip() for item in value.split(',') if item.strip()]
                    elif key == "edi_extension":
                         # Ensure extension starts with a dot
                         if value and not value.startswith('.'):
                              value = '.' + value
                         self.current_config[key] = value.lower() # Store as lowercase
                    else:
                        # Store other values (paths, etc.)
                        self.current_config[key] = value.strip() if isinstance(value, str) else value

            print("DEBUG: Calling save_callback (save_configuration) from GeneralConfigWindow")
            # Call the main save function passed during initialization
            if self.save_callback(self.current_config):
                messagebox.showinfo("Saved", "General settings saved successfully.", parent=self)
                self.destroy() # Close window on successful save
            else:
                # Error message should be shown by save_configuration itself
                messagebox.showerror("Error", "Failed to save configuration. Check logs.", parent=self)
        except Exception as e:
             messagebox.showerror("Error", f"An error occurred while saving settings: {e}", parent=self)
             print(f"ERROR in GeneralConfigWindow._save_settings: {e}")


# ==========================================================
# Notification Settings GUI Window Class
# ==========================================================
class NotificationConfigWindow(tk.Toplevel):
    """GUI window for editing email notification settings."""
    def __init__(self, master, current_config: Dict, save_callback: callable):
        super().__init__(master)
        self.title("Notification Settings")
        self.geometry("600x400")
        self.transient(master)
        self.grab_set()

        self.current_config = current_config
        self.save_callback = save_callback
        self.notif_vars = {} # Dictionary to hold the tkinter variables

        # --- GUI Layout ---
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Ensure 'notification_settings' exists in config, using defaults if necessary
        if 'notification_settings' not in self.current_config or not isinstance(self.current_config['notification_settings'], dict):
             self.current_config['notification_settings'] = DEFAULT_NOTIFICATION_SETTINGS.copy()
        current_notif_settings = self.current_config['notification_settings']

        # Create form elements based on DEFAULT_NOTIFICATION_SETTINGS keys for order and type
        settings_keys = DEFAULT_NOTIFICATION_SETTINGS.keys()
        for row, key in enumerate(settings_keys):
            label_text = key.replace('_', ' ').title() + ":"
            ttk.Label(main_frame, text=label_text).grid(row=row, column=0, sticky=tk.W, padx=5, pady=3)

            # Get the default value type to decide widget type
            default_value = DEFAULT_NOTIFICATION_SETTINGS[key]
            # Get the current value, falling back to default if missing in current settings
            current_value = current_notif_settings.get(key, default_value)

            # Create appropriate widget (Checkbutton for bool, Entry for others)
            if isinstance(default_value, bool):
                var = tk.BooleanVar(value=current_value)
                widget = ttk.Checkbutton(main_frame, variable=var)
                widget.grid(row=row, column=1, sticky=tk.W, padx=5, pady=3)
            else: # Assume string for others
                var = tk.StringVar(value=str(current_value)) # Ensure value is string for Entry
                widget = ttk.Entry(main_frame, textvariable=var, width=50)
                widget.grid(row=row, column=1, sticky=tk.EW, padx=5, pady=3)

            self.notif_vars[key] = var # Store the variable

        # Configure column 1 (widgets) to expand
        main_frame.columnconfigure(1, weight=1)

        # --- Save/Cancel Buttons ---
        button_frame = ttk.Frame(main_frame)
        # Place below the last setting row
        button_frame.grid(row=len(settings_keys), column=0, columnspan=2, pady=(15, 0))
        ttk.Button(button_frame, text="Save", command=self._save_settings).pack(side=tk.RIGHT, padx=5)
        ttk.Button(button_frame, text="Cancel", command=self.destroy).pack(side=tk.RIGHT)

    def _save_settings(self):
        """Updates the notification settings in the main config and saves."""
        print("DEBUG: NotificationConfigWindow._save_settings called")
        try:
            # Collect new settings from the tk variables
            new_settings = {}
            for key, var in self.notif_vars.items():
                new_settings[key] = var.get() # .get() works for BooleanVar and StringVar

            # Update the notification_settings dictionary within the main config
            self.current_config['notification_settings'].update(new_settings)

            print("DEBUG: Calling save_callback from NotificationConfigWindow")
            # Call the main save function
            if self.save_callback(self.current_config):
                messagebox.showinfo("Saved", "Notification settings saved successfully.", parent=self)
                self.destroy() # Close on success
            else:
                 messagebox.showerror("Error", "Failed to save notification settings. Check logs.", parent=self)
        except Exception as e:
             messagebox.showerror("Error", f"An error occurred while saving notification settings: {e}", parent=self)
             print(f"ERROR in NotificationConfigWindow._save_settings: {e}")


# ==========================================================
# Client Profile Add/Edit GUI Window Class
# ==========================================================
class ClientProfileWindow(tk.Toplevel):
    """GUI window for adding or editing a client profile."""
    def __init__(self, master, client_name: Optional[str] = None, client_data: Optional[Dict] = None, existing_names: Optional[list] = None, callback: Optional[callable] = None):
        super().__init__(master)
        self.client_name = client_name # Original name if editing, None if adding
        self.client_data = DEFAULT_CLIENT_PROFILE.copy() # Start with defaults
        if client_data:
            self.client_data.update(client_data) # Load provided data over defaults

        self.existing_names = existing_names or [] # List of other client names
        self.save_callback = callback # Function to call on successful save (from ClientConfigWindow)

        # Window setup
        self.title(f"Edit Client Profile: {self.client_name}" if self.client_name else "Add New Client Profile")
        self.geometry("600x480")
        self.transient(master)
        self.grab_set()

        self.vars = {} # Dictionary to hold tkinter variables for fields

        # --- GUI Layout ---
        main_frame = ttk.Frame(self, padding=10)
        main_frame.pack(expand=True, fill=tk.BOTH)

        # --- Client Name ---
        name_frame = ttk.Frame(main_frame)
        name_frame.pack(fill=tk.X, pady=5)
        ttk.Label(name_frame, text="Client Name:", width=15).pack(side=tk.LEFT, padx=5)
        self.name_var = tk.StringVar(value=self.client_name or "")
        # Prevent renaming existing clients via this window
        name_entry_state = 'readonly' if self.client_name else 'normal'
        name_entry = ttk.Entry(name_frame, textvariable=self.name_var, state=name_entry_state)
        name_entry.pack(side=tk.LEFT, expand=True, fill=tk.X, padx=5)

        # --- Folder Paths ---
        folder_frame = ttk.LabelFrame(main_frame, text="Folder Paths (Leave blank to use General Settings)", padding=5)
        folder_frame.pack(fill=tk.X, pady=5)
        # Define keys for path entries
        path_keys = {
            "source_folder": "Source Folder*:", # Indicate mandatory for profile logic?
            "processing_folder": "Processing Folder:",
            "processed_folder": "Processed Folder:",
            "secured_folder": "Secured Folder:",
            "error_folder": "Error Folder:"
        }
        for key, label in path_keys.items():
            self._create_path_entry(folder_frame, key, label)

        # --- FTP Destination ---
        ftp_frame = ttk.LabelFrame(main_frame, text="FTP Destination (Optional)", padding=5)
        ftp_frame.pack(fill=tk.X, pady=5)
        # Define keys for FTP entries
        ftp_keys = {
            "ftp_host": "Host:",
            "ftp_user": "Username:",
            "ftp_password": "Password:",
            "ftp_remote_path": "Remote Path:"
        }
        for key, label in ftp_keys.items():
            # Show asterisks for password field
            show_char = '*' if key == "ftp_password" else None
            self._create_entry(ftp_frame, key, label, show_char=show_char)

        # FTP Port Entry (using IntVar)
        port_frame = ttk.Frame(ftp_frame)
        port_frame.pack(fill=tk.X, pady=2)
        ttk.Label(port_frame, text="Port:", width=15).pack(side=tk.LEFT, padx=5)
        # Use an IntVar for the port, default to 21
        self.vars["ftp_port"] = tk.IntVar(value=self.client_data.get('ftp_port', 21))
        ttk.Entry(port_frame, textvariable=self.vars["ftp_port"], width=10).pack(side=tk.LEFT, padx=5)

        # Warning about password storage
        ttk.Label(ftp_frame, text="Warning: FTP passwords stored insecurely in config.json!", foreground="darkred").pack(fill=tk.X, padx=5, pady=2)

        # --- Save/Cancel Buttons ---
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=(10, 0))
        # THE SAVE BUTTON IS CREATED HERE:
        ttk.Button(button_frame, text="Save", command=self._save).pack(side=tk.RIGHT, padx=5)
        ttk.Button(button_frame, text="Cancel", command=self.destroy).pack(side=tk.RIGHT)

    def _create_entry(self, parent: ttk.Frame, key: str, label_text: str, show_char: Optional[str] = None):
        """Helper to create a label and entry row."""
        frame = ttk.Frame(parent)
        frame.pack(fill=tk.X, pady=2)
        ttk.Label(frame, text=label_text, width=15).pack(side=tk.LEFT, padx=5)
        current_value = self.client_data.get(key, "") # Get current value or default ""
        var = tk.StringVar(value=current_value)
        self.vars[key] = var # Store var
        entry = ttk.Entry(frame, textvariable=var, show=show_char)
        entry.pack(side=tk.LEFT, expand=True, fill=tk.X, padx=5)

    def _create_path_entry(self, parent: ttk.Frame, key: str, label_text: str):
        """Helper to create a label, entry, and browse button row for paths."""
        frame = ttk.Frame(parent)
        frame.pack(fill=tk.X, pady=2)
        ttk.Label(frame, text=label_text, width=15).pack(side=tk.LEFT, padx=5)
        current_value = self.client_data.get(key, "")
        var = tk.StringVar(value=current_value)
        self.vars[key] = var
        entry = ttk.Entry(frame, textvariable=var)
        entry.pack(side=tk.LEFT, expand=True, fill=tk.X, padx=5)
        # Add browse button for folder paths
        ttk.Button(frame, text="...", width=3, command=lambda e=entry: self._browse_folder(e)).pack(side=tk.LEFT, padx=5)

    def _browse_folder(self, entry_widget: tk.Entry):
        """Opens a directory dialog for path entries."""
        initial = entry_widget.get()
        initialdir = initial if os.path.isdir(initial) else "/"
        folder = filedialog.askdirectory(initialdir=initialdir, parent=self)
        if folder:
             entry_widget.delete(0, tk.END)
             entry_widget.insert(0, folder)

    def _save(self):
        """Validates input, prepares data, and calls the save callback."""
        new_name = self.name_var.get().strip()

        # --- Validation ---
        if not new_name:
            messagebox.showerror("Input Error", "Client Name cannot be empty.", parent=self)
            return
        # Prevent renaming via this window
        if self.client_name and new_name != self.client_name:
            messagebox.showerror("Input Error", "Renaming clients is not supported here.", parent=self)
            return
        # Prevent duplicate names when adding a new client
        # Check against the list of names passed in (excluding self if editing)
        other_names = [name for name in self.existing_names if name != self.client_name]
        if new_name in other_names:
            messagebox.showerror("Input Error", f"A client profile named '{new_name}' already exists.", parent=self)
            return

        # --- Collect Data ---
        new_data = {}
        try:
            for key, var in self.vars.items():
                value = var.get() # Get value from tk variable
                if key == 'ftp_port':
                    # Handle empty port entry -> use default 21
                    if isinstance(value, str) and not value.strip():
                        value = 21 # Default port
                    else:
                        # Convert to int, will raise ValueError if not numeric
                        value = int(value)
                    # Validate port range
                    if not (0 <= value <= 65535):
                        raise ValueError("Port number must be between 0 and 65535.")
                    new_data[key] = value
                elif key == 'ftp_password':
                    # Store password as is (string)
                    new_data[key] = value # Don't strip password
                else:
                    # Store other values (mostly strings from Entries), strip whitespace
                    new_data[key] = value.strip() if isinstance(value, str) else value

        except (tk.TclError, ValueError) as e:
            # Catch errors converting port to int or other potential Tcl errors
            error_field = key # The last key processed before error
            messagebox.showerror("Input Error", f"Invalid value for '{error_field.replace('_',' ').title()}':\n{e}", parent=self)
            return

        # --- Callback ---
        if self.save_callback:
            print(f"DEBUG: ClientProfileWindow calling save callback for '{new_name}'")
            # Pass the new name, the collected data, and the original name (if editing)
            self.save_callback(new_name, new_data, original_name=self.client_name)
            self.destroy() # Close the profile window after calling back
        else:
            messagebox.showerror("Internal Error", "Save callback function is missing.", parent=self)


# ==========================================================
# Client Profile Management Window Class
# ==========================================================
class ClientConfigWindow(tk.Toplevel):
    """GUI window for managing client profiles (add, edit, delete, set active)."""
    def __init__(self, master, save_app_config_func: callable, log_func: callable):
        super().__init__(master)
        # Store references passed from the main app
        self.master = master
        self.save_app_config = save_app_config_func # Function to save the *entire* config
        self.log_to_parent = log_func # Function to log to main app

        # Window setup
        self.title("Client Profiles Management")
        self.geometry("550x400")
        self.transient(master)
        self.grab_set()

        # Load current config (using the globally defined function)
        self.app_config = load_configuration()
        self.client_profiles = self.app_config.get("clients", {})
        self.active_client_name = self.app_config.get("active_client")

        # --- GUI Layout ---
        main_frame = ttk.Frame(self, padding=10)
        main_frame.pack(expand=True, fill=tk.BOTH)
        main_frame.rowconfigure(0, weight=1)    # Allow listbox area to expand vertically
        main_frame.columnconfigure(0, weight=1) # Allow listbox area to expand horizontally

        # --- Listbox Frame ---
        list_frame = ttk.Frame(main_frame)
        list_frame.grid(row=0, column=0, columnspan=2, sticky="nsew", pady=(0, 10))
        list_frame.rowconfigure(0, weight=1)    # Allow listbox to expand within its frame
        list_frame.columnconfigure(0, weight=1) # Allow listbox to expand

        # Client Listbox
        self.client_listbox = tk.Listbox(list_frame, height=10, exportselection=False)
        self.client_listbox.grid(row=0, column=0, sticky="nsew")

        # Scrollbar for Listbox
        scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.client_listbox.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.client_listbox.config(yscrollcommand=scrollbar.set)

        # Bind events to listbox actions
        self.client_listbox.bind("<<ListboxSelect>>", self._on_select) # Selection change
        self.client_listbox.bind("<Double-Button-1>", self._edit_selected) # Double-click

        # --- Button Frame ---
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=1, column=0, columnspan=2, sticky="ew") # Place below list

        # Action Buttons (packed within the button_frame)
        self.add_button = ttk.Button(button_frame, text="Add Client", command=self._add_client)
        self.add_button.pack(side=tk.LEFT, padx=5)

        self.edit_button = ttk.Button(button_frame, text="Edit Selected", command=self._edit_selected, state=tk.DISABLED)
        self.edit_button.pack(side=tk.LEFT, padx=5)

        self.delete_button = ttk.Button(button_frame, text="Delete Selected", command=self._delete_selected, state=tk.DISABLED)
        self.delete_button.pack(side=tk.LEFT, padx=5)

        self.set_active_button = ttk.Button(button_frame, text="Set Active & Close", command=self._set_active_and_close, state=tk.DISABLED)
        self.set_active_button.pack(side=tk.RIGHT, padx=5) # Pack to the right

        # Initial population of the listbox
        self._populate_list()

    # --- Methods (Correctly Indented) ---

    def _populate_list(self):
        """Clears and refills the listbox with client names."""
        self.client_listbox.delete(0, tk.END) # Clear existing items
        selected_index = -1
        client_names = sorted(self.client_profiles.keys()) # Get sorted list

        for i, name in enumerate(client_names):
            display_name = name
            if name == self.active_client_name:
                display_name += " (Active)"
                selected_index = i
            self.client_listbox.insert(tk.END, display_name)

        if selected_index != -1:
            self.client_listbox.selection_set(selected_index)
            self.client_listbox.activate(selected_index)
            self.client_listbox.see(selected_index)

        self._update_button_states()

    def _on_select(self, event=None):
        """Callback when listbox selection changes."""
        self._update_button_states()

    def _update_button_states(self):
        """Enables/disables buttons based on listbox selection."""
        has_selection = bool(self.client_listbox.curselection())
        new_state = tk.NORMAL if has_selection else tk.DISABLED
        # Safely configure buttons if they exist
        if hasattr(self, 'edit_button'): self.edit_button.config(state=new_state)
        if hasattr(self, 'delete_button'): self.delete_button.config(state=new_state)
        if hasattr(self, 'set_active_button'): self.set_active_button.config(state=new_state)

    def _get_selected_client_name(self):
        """Returns the actual name of the selected client (without '(Active)')."""
        indices = self.client_listbox.curselection()
        # Check if any item is selected
        if not indices: # Corrected this block's indentation
            return None
        # Get the text of the first selected item
        selected_item = self.client_listbox.get(indices[0])
        # Remove the marker and return the cleaned name
        return selected_item.replace(" (Active)", "").strip()

    def _add_client(self):
        """Opens the ClientProfileWindow for adding a new client."""
        ClientProfileWindow(
            self, # Parent window
            existing_names=list(self.client_profiles.keys()), # Pass existing names
            callback=self._save_profile # Pass the method to call on save
        )

    def _edit_selected(self, event=None):
        """Opens the ClientProfileWindow to edit the selected client."""
        name = self._get_selected_client_name()
        # Check if a client was selected
        if not name: # Corrected indentation
            messagebox.showwarning("Selection Error", "Please select a client profile to edit.", parent=self)
            return
        # Get data for the selected client
        data = self.client_profiles.get(name)
        # Check if data exists
        if not data: # Corrected indentation
            messagebox.showerror("Error", f"Could not find configuration data for '{name}'.", parent=self)
            return
        # Open the profile window for editing
        ClientProfileWindow( # Corrected indentation
            self, # Parent
            client_name=name, # Pass name being edited
            client_data=data, # Pass current data
            existing_names=list(self.client_profiles.keys()),
            callback=self._save_profile # Use same save callback
        )

    def _save_profile(self, client_name, client_data, original_name=None):
        """Callback from ClientProfileWindow to save changes."""
        # Prevent renaming
        if original_name and original_name != client_name: # Corrected indentation
            messagebox.showerror("Error", "Renaming clients is not supported here.", parent=self)
            return
        # Update the profile data locally
        self.client_profiles[client_name] = client_data
        self.app_config['clients'] = self.client_profiles # Update main config dict
        action = "Added" if original_name is None else "Updated"
        # Attempt to save the entire configuration file
        if self.save_app_config(self.app_config): # Corrected indentation
            self.log_to_parent(f"{action} client profile: {client_name}")
            # Reload config locally and refresh list after successful save
            self.app_config = load_configuration()
            self.client_profiles = self.app_config.get("clients", {})
            self.active_client_name = self.app_config.get("active_client")
            self._populate_list()
        else:
            self.log_to_parent(f"ERROR saving configuration after {action.lower()} profile: {client_name}", "ERROR")

    def _delete_selected(self):
        """Deletes the selected client profile."""
        name = self._get_selected_client_name()
        # Check if a client is selected
        if not name: # Corrected indentation
            messagebox.showwarning("Selection Error", "Please select a client profile to delete.", parent=self)
            return
        # Confirm deletion
        if messagebox.askyesno("Confirm Deletion", f"Are you sure you want to delete the profile for '{name}'?", parent=self): # Corrected indentation
            # Delete from local dictionary
            if name in self.client_profiles:
                del self.client_profiles[name]
            # If the deleted client was active, reset active client in config
            if self.active_client_name == name:
                self.active_client_name = None
                self.app_config['active_client'] = None
            # Update the main config object
            self.app_config['clients'] = self.client_profiles
            # Attempt to save the configuration change
            if self.save_app_config(self.app_config):
                self.log_to_parent(f"Deleted client profile: {name}")
                # Reload local state and refresh list on success
                self.app_config = load_configuration()
                self.client_profiles = self.app_config.get("clients", {})
                self.active_client_name = self.app_config.get("active_client")
                self._populate_list()
            else:
                self.log_to_parent(f"ERROR saving configuration after deleting profile: {name}", "ERROR")
                # Reload config to revert local deletion if save fails
                self.app_config = load_configuration()
                self.client_profiles = self.app_config.get("clients",{})
                self.active_client_name = self.app_config.get("active_client")
                self._populate_list() # Refresh list to show reverted state

    def _set_active_and_close(self):
        """Sets the selected client as active, saves config, and closes the window."""
        name = self._get_selected_client_name()
        # Check if a client is selected
        if not name: # Corrected indentation
             messagebox.showwarning("Selection Error", "Please select a client profile to activate.", parent=self)
             return
        # Update the active client in the main config object
        self.app_config['active_client'] = name
        # Attempt to save the configuration change
        if self.save_app_config(self.app_config): # Corrected indentation
            self.log_to_parent(f"Client profile '{name}' set as active.")
            self.destroy() # Close this window on success
        else:
            self.log_to_parent(f"ERROR saving configuration after setting active client.", "ERROR")


# ==========================================================
# Main Application GUI Class
# ==========================================================
class EDIProcessorGUI:
    """Main application class for the EDI Processor GUI."""

    def __init__(self, master: tk.Tk):
        """Initializes the main application window."""
        self.master = master
        master.title("EDI Processor") # Initial title
        master.geometry("900x700") # Initial size

        # --- Load Initial Configuration ---
        # Load configuration using the global function
        self.config = load_configuration()

        # --- Tkinter Variables ---
        # Variables for schedule settings widgets
        self.interval_var = tk.IntVar(value=self.config.get('schedule_interval_minutes', 5)) # Default 5 min
        self.auto_restart_var = tk.BooleanVar(value=self.config.get('auto_restart_schedule', True))

        # --- Internal State ---
        self.progress_window: Optional[tk.Toplevel] = None
        self.progress_bar: Optional[ttk.Progressbar] = None
        self.progress_label: Optional[ttk.Label] = None
        self.file_label: Optional[ttk.Label] = None
        self.progress_mode: str = 'indeterminate'
        self.progress_var: Optional[tk.DoubleVar] = None

        self.scheduled_processing_running: bool = False # Flag for schedule status
        self.schedule_thread: Optional[threading.Thread] = None # Holds the schedule thread object
        self._manual_processing_active: bool = False # Flag for manual run status

        self.edit_instructions_window: Optional[tk.Toplevel] = None # Reference to instructions window

        # --- GUI Setup ---
        pad_options = {'padx': 10, 'pady': 5} # Padding options for main frames

        # Log Output Area
        log_frame = ttk.LabelFrame(master, text="Log Output", padding=(5, 5))
        log_frame.pack(fill=tk.BOTH, expand=True, **pad_options)
        self.log_text = scrolledtext.ScrolledText(log_frame, height=15, wrap=tk.WORD, font=("Consolas", 9), state='disabled') # Start disabled
        self.log_text.pack(pady=5, padx=5, fill=tk.BOTH, expand=True)

        # Control Buttons Frame (Manual Run, Schedule, Stop)
        control_frame = ttk.Frame(master)
        control_frame.pack(fill=tk.X, **pad_options)

        self.process_button = ttk.Button(control_frame, text="Manual Run Now", command=self.start_processing)
        self.process_button.pack(side=tk.LEFT, padx=5)

        self.schedule_button = ttk.Button(control_frame, text="Start Schedule", command=self.start_scheduled_processing)
        self.schedule_button.pack(side=tk.LEFT, padx=5)

        self.stop_schedule_button = ttk.Button(control_frame, text="Stop Schedule", command=self.stop_scheduled_processing, state=tk.DISABLED)
        self.stop_schedule_button.pack(side=tk.LEFT, padx=5)

        # Settings Buttons Frame (Grouped to the right)
        settings_buttons_frame = ttk.Frame(control_frame)
        settings_buttons_frame.pack(side=tk.RIGHT, padx=5)

        ttk.Button(settings_buttons_frame, text="Clients", command=self.open_client_profiles).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(settings_buttons_frame, text="Settings", command=self.open_general_settings).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(settings_buttons_frame, text="Notifications", command=self.open_notification_settings).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(settings_buttons_frame, text="Instructions", command=self.edit_special_instructions).pack(side=tk.LEFT, padx=0)

        # Schedule Configuration Frame
        schedule_config_frame = ttk.LabelFrame(master, text="Schedule Settings", padding=5)
        schedule_config_frame.pack(fill=tk.X, **pad_options)

        ttk.Label(schedule_config_frame, text="Interval (minutes):").pack(side=tk.LEFT, padx=(5, 2))
        ttk.Entry(schedule_config_frame, textvariable=self.interval_var, width=5).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Checkbutton(schedule_config_frame, text="Auto-restart on launch", variable=self.auto_restart_var).pack(side=tk.LEFT, padx=5)
        ttk.Button(schedule_config_frame, text="Save", command=self.save_settings_to_config, width=8).pack(side=tk.RIGHT, padx=5)

        # Status Bar
        self.status_bar = ttk.Frame(master, relief=tk.SUNKEN, padding=(2, 1))
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)
        self.status_label = ttk.Label(self.status_bar, text="Status: Ready")
        self.status_label.pack(side=tk.LEFT, padx=5)

        # --- Initial Setup ---
        self.update_active_client_display() # Set initial title and status bar text
        self.log_message("Application initialized.")

        # Auto-restart logic based on loaded config and state file
        if self.auto_restart_var.get():
            if self.load_schedule_state(): # Check state file
                self.log_message("Auto-restart enabled and previous state was running. Restarting schedule...")
                # Use master.after to start schedule shortly after main loop starts
                self.master.after(500, self.start_scheduled_processing)
            else:
                self.log_message("Auto-restart enabled, but previous state was not running.")
        else:
            self.log_message("Auto-restart is disabled in configuration.")
        # --- END OF __init__ ---


    # --- Logging Method ---
    def log_message(self, message: str, level: str = "INFO"):
        """Logs a message to the console and the GUI log text area."""
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"[{timestamp}] [{level.upper()}] {message}\n"
        print(log_entry.strip()) # Always print to console

        # Schedule the GUI update on the main thread safely
        try:
            if hasattr(self, 'log_text') and self.log_text.winfo_exists():
                # Use master.after to ensure GUI updates happen on the main thread
                self.master.after(0, self._insert_log, log_entry)
        except Exception as e:
            print(f"GUI Log Error: Could not schedule log update: {e}")

    def _insert_log(self, log_entry: str):
        """Inserts the log entry into the ScrolledText widget (called via master.after)."""
        try:
            if hasattr(self, 'log_text') and self.log_text.winfo_exists():
                self.log_text.configure(state='normal') # Temporarily enable
                self.log_text.insert(tk.END, log_entry)
                self.log_text.see(tk.END) # Scroll to the end
                self.log_text.configure(state='disabled') # Disable again
            # else: Widget might have been destroyed during shutdown
        except Exception as e:
            print(f"GUI Log Error: Failed to insert log into widget: {e}")


    # --- Progress Window Methods ---
    def show_progress_window(self, total_files: int = 0):
        """Creates or brings focus to the progress window."""
        # If window exists, just bring it to the front
        if self.progress_window and tk.Toplevel.winfo_exists(self.progress_window):
            self.progress_window.lift()
            return

        # Create the Toplevel window
        self.progress_window = tk.Toplevel(self.master)
        self.progress_window.title("Processing Progress")
        self.progress_window.geometry("400x150")
        self.progress_window.resizable(False, False)
        self.progress_window.transient(self.master) # Keep on top of master
        self.progress_window.grab_set() # Make modal (prevents interaction with main window)
        # Handle user closing the window manually
        self.progress_window.protocol("WM_DELETE_WINDOW", self._handle_progress_close)

        # Determine progress bar mode based on whether total files is known
        if total_files > 0:
            self.progress_mode = 'determinate'
            self.progress_var = tk.DoubleVar()
            self.progress_bar = ttk.Progressbar(self.progress_window, variable=self.progress_var, maximum=100, mode=self.progress_mode)
        else:
            self.progress_mode = 'indeterminate'
            self.progress_var = None # Not used in indeterminate mode
            self.progress_bar = ttk.Progressbar(self.progress_window, mode=self.progress_mode)

        self.progress_bar.pack(pady=20, padx=20, fill=tk.X)

        # Start animation if indeterminate
        if self.progress_mode == 'indeterminate':
            self.progress_bar.start(10) # Animation interval

        # Labels for status text and current file
        self.progress_label = ttk.Label(self.progress_window, text="Preparing...", anchor=tk.W)
        self.progress_label.pack(pady=5, padx=20, fill=tk.X)
        self.file_label = ttk.Label(self.progress_window, text="File: N/A", anchor=tk.W)
        self.file_label.pack(pady=0, padx=20, fill=tk.X)

    def update_progress(self, value: Optional[float] = None, text: str = "Processing...", current_file: str = "N/A"):
        """Updates the progress bar and labels. Should be called via master.after from threads."""
        try:
            # Check if progress window and its components still exist
            if (hasattr(self,'progress_window') and self.progress_window and
                tk.Toplevel.winfo_exists(self.progress_window) and self.progress_bar):

                # Update determinate progress bar value
                if self.progress_mode == 'determinate' and self.progress_var is not None and value is not None:
                     # Clamp value between 0 and 100
                     clamped_value = max(0.0, min(100.0, value))
                     self.progress_var.set(clamped_value)

                # Update labels
                if hasattr(self, 'progress_label'): self.progress_label.config(text=text)
                if hasattr(self, 'file_label'): self.file_label.config(text=f"File: {current_file}")

                # Force GUI update
                self.progress_window.update_idletasks()
        except Exception as e:
            self.log_message(f"Progress Update Error: {e}", "ERROR")

    def _handle_progress_close(self):
        """Handles the user manually closing the progress window via the [X] button."""
        self.log_message("Progress window closed manually by user.", "WARN")
        # NOTE: This currently doesn't stop the background processing thread.
        # Implementing cancellation requires more complex thread communication (e.g., threading.Event).
        self.close_progress_window(force_enable_buttons=True) # Force enable buttons

    def close_progress_window(self, force_enable_buttons: bool = False):
        """Closes the progress window and cleans up resources."""
        if hasattr(self,'progress_window') and self.progress_window:
            try:
                if tk.Toplevel.winfo_exists(self.progress_window):
                    # Stop indeterminate animation before destroying
                    if self.progress_mode == 'indeterminate' and self.progress_bar:
                        self.progress_bar.stop()
                    self.progress_window.grab_release() # Release grab
                    self.progress_window.destroy()
            except Exception as e:
                self.log_message(f"Progress Close Error: {e}", "ERROR")
            finally:
                 # Ensure references are cleared even if destroy fails
                 self.progress_window = None
                 self.progress_bar = None
                 self.progress_label = None
                 self.file_label = None
                 self.progress_var = None

        # Re-enable main control buttons if processing is finished or forced
        if force_enable_buttons or not (self.scheduled_processing_running or self._manual_processing_active):
             self._enable_control_buttons()

    def _enable_control_buttons(self):
         """Safely enables control buttons (Manual Run, Start Schedule) and sets Stop Schedule state."""
         try:
            # Check if widgets exist before configuring (important during shutdown)
            if hasattr(self, 'process_button') and self.process_button.winfo_exists():
                self.process_button.config(state=tk.NORMAL)
            if hasattr(self, 'schedule_button') and self.schedule_button.winfo_exists():
                self.schedule_button.config(state=tk.NORMAL)
            if hasattr(self, 'stop_schedule_button') and self.stop_schedule_button.winfo_exists():
                # Stop button should only be enabled if the schedule is actually running
                stop_state = tk.NORMAL if self.scheduled_processing_running else tk.DISABLED
                self.stop_schedule_button.config(state=stop_state)
         except tk.TclError as e:
             print(f"WARN: Error enabling control buttons (likely during shutdown): {e}")
         except Exception as e:
              self.log_message(f"Error enabling control buttons: {e}", "ERROR")


    # --- Manual Processing Methods ---
    def start_processing(self):
        """Initiates a manual, one-time processing run in a separate thread."""
        # Prevent multiple runs concurrently
        if self._manual_processing_active:
            messagebox.showwarning("Busy", "A manual process is already running.", parent=self.master)
            return
        if self.scheduled_processing_running:
            messagebox.showwarning("Busy", "Scheduled processing is active. Stop the schedule first.", parent=self.master)
            return

        # --- Start Manual Run ---
        self._manual_processing_active = True
        # Disable buttons to prevent interference
        self.process_button.config(state=tk.DISABLED)
        self.schedule_button.config(state=tk.DISABLED)
        self.stop_schedule_button.config(state=tk.DISABLED) # Disable stop during manual run

        self.log_message("Starting manual processing run...")
        self.update_status("Manual run starting...")

        # Reload config and validate before starting thread
        self.config = load_configuration()
        self.update_active_client_display()
        active_settings = self._get_active_settings()
        if not self._validate_active_config(active_settings, log_prefix="Manual Run"):
             self._finalize_manual_processing() # Reset state if config invalid
             return

        # Start the processing task in a background thread
        threading.Thread(target=self._process_edi_files_once, daemon=True).start()

    def _process_edi_files_once(self):
        """Target function for the manual processing thread."""
        success_count, fail_count, skipped_count = 0, 0, 0
        try:
            # Get effective settings again within the thread
            active_settings = self._get_active_settings()
            # Re-validate just in case config changed between button click and thread start
            if not self._validate_active_config(active_settings):
                 raise ValueError("Configuration became invalid before processing could start.")

            # --- Call Core Processing Logic ---
            success_count, fail_count, skipped_count = self._process_edi_files(config_override=active_settings)

            # Show results popup (scheduled on main thread)
            self.master.after(0, self.show_completion_popup, success_count, fail_count, skipped_count)

        except Exception as e:
            # Log detailed error from the processing thread
            error_message = f"Manual run failed: {e}\n{traceback.format_exc()}"
            self.master.after(0, self.log_message, error_message, "ERROR")
            # Show error popup to user (scheduled on main thread)
            self.master.after(0, messagebox.showerror, "Processing Error", f"An error occurred during manual processing:\n{e}", parent=self.master)

            # Attempt to send error notification (using current config for settings)
            try:
                active_settings = self._get_active_settings() # Get settings again for notification
                notif_cfg = active_settings.get('notification_settings', {})
                if notif_cfg.get('send_on_error'):
                    details = {"error": error_message, "context": "Manual Processing Run"}
                    subject = notif_cfg.get('subject_error', 'EDI Process Error')
                    body = self._build_email_body('error', details, config_override=active_settings)
                    # Schedule notification sending on main thread
                    self.master.after(0, self.send_notification_email, subject, body)
            except Exception as notif_err:
                 # Log if sending the error notification itself fails
                 self.master.after(0, self.log_message, f"Failed to send error notification for manual run: {notif_err}", "ERROR")
        finally:
            # --- Finalize Manual Run ---
            # Ensure state is reset and GUI elements updated on main thread
            self.master.after(0, self._finalize_manual_processing)

    def _finalize_manual_processing(self):
        """Resets state after manual processing finishes or fails."""
        self._manual_processing_active = False
        self.close_progress_window(force_enable_buttons=True) # Close progress, enable buttons
        self.log_message("Manual processing run finished.")
        self.update_status("Ready") # Update status bar

    def show_completion_popup(self, success_count: int, fail_count: int, skipped_count: int):
        """Displays the results popup and sends summary notification if enabled."""
        message = f"Processing Complete!\n\n" \
                  f"Successfully processed: {success_count}\n" \
                  f"Failed: {fail_count}\n" \
                  f"Skipped/Ignored: {skipped_count}"
        messagebox.showinfo("Processing Results", message, parent=self.master)

        # Send summary notification if enabled (using current settings)
        try:
            active_settings = self._get_active_settings()
            notif_cfg = active_settings.get('notification_settings', {})
            if notif_cfg.get('send_on_success_summary', False):
                 # Determine context based on current state flags
                 context = "Manual Run Summary" if self._manual_processing_active else "Scheduled Run Summary"
                 # Only send summary if there was some activity
                 if success_count > 0 or fail_count > 0 or skipped_count > 0:
                     details = {
                         "success_count": success_count,
                         "fail_count": fail_count,
                         "skipped_count": skipped_count,
                         "context": context
                     }
                     subject = notif_cfg.get('subject_summary', 'EDI Process Batch Summary')
                     body = self._build_email_body('summary', details, config_override=active_settings)
                     # Can call directly as this method is called via master.after
                     self.send_notification_email(subject, body)
        except Exception as e:
             self.log_message(f"Failed to send summary notification: {e}", "ERROR")


    # --- Scheduled Processing Methods ---
    def start_scheduled_processing(self):
        """Starts the scheduled processing loop in a separate thread."""
        # Prevent multiple schedule runs or running during manual process
        if self.scheduled_processing_running:
            messagebox.showinfo("Info", "Scheduled processing is already running.", parent=self.master)
            return
        if self._manual_processing_active:
            messagebox.showwarning("Busy", "Cannot start schedule while a manual run is active.", parent=self.master)
            return

        # --- Start Schedule ---
        # Reload config and validate before starting
        self.config = load_configuration()
        self.update_active_client_display()
        active_settings = self._get_active_settings()
        if not self._validate_active_config(active_settings, log_prefix="Schedule Start"):
             return # Validation failed, message shown

        # Update GUI state: Disable run/schedule, enable stop
        self.process_button.config(state=tk.DISABLED)
        self.schedule_button.config(state=tk.DISABLED)
        self.stop_schedule_button.config(state=tk.NORMAL)

        self.log_message("Starting scheduled processing...")
        self.scheduled_processing_running = True # Set running flag
        self.save_schedule_state(True) # Persist running state to file

        # Update config with current GUI values for interval/auto-restart
        if not self._update_config_from_gui():
             self.log_message("Warning: Could not read schedule settings from GUI. Using loaded config values.", "WARN")
        # Save potentially updated config (interval, auto-restart)
        save_configuration(self.config)

        # Get validated interval
        interval_minutes = self.interval_var.get()
        if not isinstance(interval_minutes, int) or interval_minutes <= 0:
             interval_minutes = DEFAULT_CONFIG['schedule_interval_minutes'] # Fallback
             self.log_message(f"Invalid schedule interval. Using default: {interval_minutes} minutes.", "WARN")
             self.interval_var.set(interval_minutes) # Update GUI

        # Check if thread exists and is alive (shouldn't be, but safety check)
        if self.schedule_thread and self.schedule_thread.is_alive():
            self.log_message("Scheduler thread already exists and is alive. Not starting new thread.", "WARN")
        else:
            # Create and start the daemon thread for the schedule loop
            self.schedule_thread = threading.Thread(
                target=self._run_scheduled_processing,
                args=(interval_minutes,),
                daemon=True # Allows app to exit even if thread is waiting
            )
            self.schedule_thread.start()
            self.update_status(f"Schedule active (Interval: {interval_minutes} min)")

    def stop_scheduled_processing(self):
        """Stops the scheduled processing loop."""
        if not self.scheduled_processing_running:
            messagebox.showinfo("Info", "Scheduled processing is not currently running.", parent=self.master)
            return

        self.log_message("Stopping scheduled processing...")
        self.scheduled_processing_running = False # Signal the loop thread to stop
        self.save_schedule_state(False) # Persist stopped state

        # Schedule the GUI updates after a short delay to allow thread to potentially finish cycle
        self.master.after(100, self._finalize_schedule_stop)

    def _finalize_schedule_stop(self):
         """Updates GUI after schedule stop signal is sent."""
         self._enable_control_buttons() # Re-enable run/schedule, disable stop
         self.close_progress_window() # Close progress if it was left open
         self.log_message("Scheduled processing stopped.")
         self.update_status("Ready (Schedule stopped)")
         # Optional: Check if thread actually finished cleanly
         # if self.schedule_thread and self.schedule_thread.is_alive():
         #      self.log_message("Warning: Schedule thread may still be running.", "WARN")
         self.schedule_thread = None # Clear thread reference

    def _run_scheduled_processing(self, interval_minutes: int):
        """The main loop for scheduled processing, run in a background thread."""
        # Log start from within the thread (via master.after for GUI safety)
        self.master.after(0, self.log_message, f"Scheduler thread started. Interval: {interval_minutes} minutes.")

        while self.scheduled_processing_running:
            run_start_time = time.time()
            try:
                # --- Check Configuration within the loop ---
                # Load fresh config snapshot for this cycle
                current_config_snapshot = load_configuration()
                active_settings_snapshot = self._get_active_settings(base_config=current_config_snapshot)
                active_client_name = current_config_snapshot.get('active_client', 'None')

                # Validate config before processing
                if not self._validate_active_config(active_settings_snapshot, log_prefix="Sched Run"):
                    self.master.after(0, self.log_message, f"Sched Run: Skipping cycle due to invalid config for '{active_client_name}'.", "WARN")
                else:
                    # Log check start and update status
                    self.master.after(0, self.log_message, f"Sched Run: Checking for files for client '{active_client_name}'...")
                    self.master.after(0, self.update_status, f"Schedule active (Running check...)")

                    # --- Execute Core Processing Logic ---
                    s_c, f_c, k_c = self._process_edi_files(config_override=active_settings_snapshot)

                    # Log results (scheduled on main thread)
                    self.master.after(0, self.log_message, f"Sched Run Completed: Success={s_c}, Failed={f_c}, Skipped={k_c}.")

                    # --- Send Summary Notification (if enabled and activity occurred) ---
                    notif_cfg = active_settings_snapshot.get('notification_settings', {})
                    if notif_cfg.get('send_on_success_summary', False) and (s_c > 0 or f_c > 0 or k_c > 0):
                        details = {"success_count": s_c, "fail_count": f_c, "skipped_count": k_c, "context":"Scheduled Run"}
                        subject = notif_cfg.get('subject_summary', 'EDI Process Batch Summary')
                        body = self._build_email_body('summary', details, config_override=active_settings_snapshot)
                        self.master.after(0, self.send_notification_email, subject, body)

                    # Update status bar back to idle state (scheduled on main thread)
                    self.master.after(0, self.update_status, f"Schedule active (Interval: {interval_minutes} min)")

            except Exception as e:
                 # Catch unexpected errors in the loop itself or during processing
                 error_message = f"CRITICAL SCHEDULER LOOP ERROR: {e}\n{traceback.format_exc()}"
                 self.master.after(0, self.log_message, error_message, "ERROR")
                 self.master.after(0, self.update_status, f"Schedule ERROR occurred!")

                 # Attempt to send error notification based on *current* config
                 try:
                     current_config_snapshot = load_configuration() # Reload fresh config
                     active_settings_snapshot = self._get_active_settings(base_config=current_config_snapshot)
                     notif_cfg = active_settings_snapshot.get('notification_settings', {})
                     if notif_cfg.get('send_on_error'):
                         details = {"error": error_message, "context": "Scheduled Processing Loop"}
                         subject = notif_cfg.get('subject_error', 'EDI Process Error')
                         body = self._build_email_body('error', details, config_override=active_settings_snapshot)
                         self.master.after(0, self.send_notification_email, subject, body)
                 except Exception as notif_e:
                      self.master.after(0, self.log_message, f"Failed to send scheduler loop error notification: {notif_e}", "ERROR")

            # --- Wait for the next interval ---
            run_end_time = time.time()
            elapsed_seconds = run_end_time - run_start_time
            sleep_duration = max(0, (interval_minutes * 60) - elapsed_seconds)
            self.master.after(0, self.log_message, f"Sched Run: Took {elapsed_seconds:.2f}s. Sleeping for {sleep_duration:.2f}s.", "DEBUG")

            # Sleep in small increments to check the stop flag frequently
            sleep_chunk = 1.0 # Check every second
            end_sleep_time = time.time() + sleep_duration
            while time.time() < end_sleep_time:
                 if not self.scheduled_processing_running:
                     self.master.after(0, self.log_message, "Sched Run: Stop signal received during sleep.", "DEBUG")
                     break # Exit sleep early if stopped
                 time.sleep(sleep_chunk)

            if not self.scheduled_processing_running:
                break # Exit main loop if stopped

        # --- Loop Finished ---
        self.master.after(0, self.log_message, "Scheduler thread finished.")
        # Final state update is handled by _finalize_schedule_stop or main exit handler


    # --- Core File Processing Logic ---
    def _process_edi_files(self, config_override: Optional[Dict] = None) -> tuple[int, int, int]:
        """
        Core file processing logic. Finds files, moves, renames, parses,
        handles special instructions, uploads via FTP, and moves to final folders.

        Args:
            config_override: If provided, use this config dict instead of self.config.

        Returns:
            Tuple: (success_count, fail_count, skipped_count) for the batch.
        """
        # Determine effective configuration for this run
        cfg = config_override if config_override is not None else self._get_active_settings()

        # Extract necessary paths and settings
        src = cfg.get('source_folder')
        proc = cfg.get('processing_folder')
        pced = cfg.get('processed_folder')
        sec = cfg.get('secured_folder')
        err_fld = cfg.get('error_folder')
        ignore_kw = cfg.get('ignore_keywords', [])
        edi_ext = cfg.get('edi_extension', '.edi').lower() # Use lowercase for comparison
        instructions = cfg.get('special_instructions', [])
        notif_settings = cfg.get('notification_settings', {})
        ftp_details = cfg # FTP details are part of the effective config

        # --- Initial Validation and Folder Creation ---
        # Validate essential folder paths first
        if not self._validate_folder_paths(cfg, ["source_folder", "processing_folder", "processed_folder", "secured_folder", "error_folder"]):
             self.log_message("Core folder paths are missing or invalid in the active configuration.", "ERROR")
             return 0, 0, 0 # Cannot proceed

        # Ensure all necessary directories exist, create if missing
        for folder_path in [src, proc, pced, sec, err_fld]:
             try:
                 # exist_ok=True prevents error if directory already exists
                 os.makedirs(folder_path, exist_ok=True)
             except Exception as mkdir_e:
                  self.log_message(f"CRITICAL ERROR: Failed to create directory '{folder_path}': {mkdir_e}", "ERROR")
                  # Send notification?
                  return 0, 0, 0 # Cannot proceed if essential folders can't be created

        # --- Initialize Counters ---
        s_c, f_c, k_c = 0, 0, 0 # Success, Fail, Skip counts
        to_process = [] # List of filenames found valid for processing

        # --- File Discovery in Source Folder ---
        self.log_message(f"Scanning source folder: {src}")
        try:
            # Check source exists *before* listing
            if not os.path.isdir(src): # Use isdir for clarity
                 raise FileNotFoundError(f"Source folder does not exist or is not a directory: {src}")

            found_items = os.listdir(src)
            self.log_message(f"Found {len(found_items)} items in source folder.", "INFO")

            for item_name in found_items:
                item_path = os.path.join(src, item_name)
                # Ensure it's a file, not a directory or link
                if not os.path.isfile(item_path):
                    self.log_message(f"Skipping non-file item: '{item_name}'", "DEBUG")
                    k_c += 1
                    continue

                item_name_lower = item_name.lower()
                # Check against ignore keywords
                is_ignored = False
                for keyword in ignore_kw:
                    # Ensure keyword is not empty before checking
                    if keyword and keyword.lower() in item_name_lower:
                        self.log_message(f"Ignoring file '{item_name}' due to keyword: '{keyword}'.")
                        is_ignored = True
                        k_c += 1
                        break # Stop checking keywords for this file
                if is_ignored:
                    continue

                # Check file extension (case-insensitive)
                if not item_name_lower.endswith(edi_ext):
                    self.log_message(f"Ignoring file '{item_name}' due to incorrect extension (expected '{edi_ext}').")
                    k_c += 1
                    continue

                # If all checks pass, add filename to the list to process
                self.log_message(f"Adding '{item_name}' to processing list.", "DEBUG")
                to_process.append(item_name)

        except FileNotFoundError as e:
             # Log specific error if source folder disappears during scan
             self.log_message(f"ERROR: {e}", "ERROR")
             return s_c, f_c, k_c # Return current counts
        except Exception as e:
             # Catch other errors during listing (e.g., permissions)
             self.log_message(f"Error scanning source folder '{src}': {e}", "ERROR")
             # Count remaining potential files as skipped? Or just return current counts?
             # Returning current counts seems safer.
             return s_c, f_c, k_c

        # --- Check if Any Files Found ---
        if not to_process:
            self.log_message(f"No valid EDI files found in {src} to process in this batch.")
            # No need to show progress window if nothing to do
            return s_c, f_c, k_c

        # --- Show Progress Window (Scheduled on Main Thread) ---
        self.master.after(0, self.show_progress_window, len(to_process))

        # --- Move Files to Processing Folder ---
        moved = [] # List of filenames successfully moved
        total_to_process = len(to_process)
        for i, fn in enumerate(to_process):
            source_file_path = os.path.join(src, fn)
            processing_file_path = os.path.join(proc, fn)

            # Update progress (scheduled on main thread)
            progress_val = (i / total_to_process) * 20 # Moving phase is ~20%
            progress_text = f"Moving ({i + 1}/{total_to_process})"
            self.master.after(0, self.update_progress, progress_val, progress_text, fn)

            try:
                # Attempt the move operation
                shutil.move(source_file_path, processing_file_path)
                moved.append(fn) # Add to list of successfully moved files
                self.log_message(f"Moved '{fn}' from source to processing.", "DEBUG")

            except Exception as e: # Catch errors during move (permissions, file locked, etc.)
                f_c += 1 # Increment fail count for this file
                self.log_message(f"ERROR moving file '{fn}' from source to processing: {e}", "ERROR")

                # --- Send Notification for Move Error (if enabled) ---
                # This block is now correctly indented inside the 'except'
                if notif_settings.get('send_on_error'):
                    details = {
                        "operation": "Move to Processing",
                        "original_filename": fn,
                        "error": str(e),
                        "source": src,
                        "destination": proc
                    }
                    subject = notif_settings.get('subject_error', 'EDI Process Error')
                    body = self._build_email_body('error', details, config_override=cfg)
                    # Schedule notification sending on the main thread
                    self.master.after(0, self.send_notification_email, subject, body)
                # --- End of Notification Block ---

        # Brief pause after attempting all moves
        time.sleep(0.1)

        # If no files were successfully moved, end the processing here
        if not moved:
             self.log_message("No files were successfully moved to the processing folder.", "WARN")
             self.master.after(0, self.close_progress_window) # Ensure progress window closes
             return s_c, f_c, k_c # Return counts (only failures/skips possible)

        # --- Process Files Successfully Moved to Processing Folder ---
        f_ctr = 1 # Counter for generating unique parts of new filenames
        total_moved = len(moved)
        for i, original_filename in enumerate(moved):
            # Define paths relative to the processing folder
            processing_path = os.path.join(proc, original_filename) # Current path of the file

            # --- Prepare for Processing ---
            # Default new filename structure (timestamp + counter)
            new_base_filename = f"processed_{datetime.datetime.now():%Y%m%d_%H%M%S}_{f_ctr}"
            new_filename = new_base_filename + edi_ext # Default new name
            new_processing_path = None # Path after potential rename
            operation_stage = "Start Processing" # Track current step for error reporting
            matched_instruction = {} # Store instruction if matched

            # Update progress bar (scheduled on main thread)
            progress_val = 20 + (i / total_moved) * 60 # Processing phase is ~60%
            progress_text = f"Processing ({i + 1}/{total_moved})"
            self.master.after(0, self.update_progress, progress_val, progress_text, original_filename)

            try:
                # --- Stage 1: Check Special Instructions & Determine New Filename ---
                operation_stage = "Check Instructions"
                for instr in instructions:
                    # Check if instruction is enabled and has a 'contains' keyword
                    if instr.get("enabled") and instr.get("contains"):
                         contains_keyword = instr["contains"].lower()
                         # Check if keyword exists in the original filename (case-insensitive)
                         if contains_keyword in original_filename.lower():
                             self.log_message(f"File '{original_filename}' matches instruction rule containing '{contains_keyword}'.")
                             matched_instruction = instr # Store the matched rule

                             # Apply new filename base from instruction if provided
                             instr_new_name_base = instr.get('new_filename', '').strip()
                             if instr_new_name_base:
                                  # Construct new name: base + timestamp + counter + extension
                                  new_filename = f"{instr_new_name_base}_{datetime.datetime.now():%Y%m%d_%H%M%S}_{f_ctr}{edi_ext}"
                             else:
                                 # Use default generated name if rule matched but had no filename base
                                  new_filename = f"processed_{datetime.datetime.now():%Y%m%d_%H%M%S}_{f_ctr}{edi_ext}"
                             break # Stop checking rules once one matches
                # If no instruction matched, n_fn remains the default generated name
                f_ctr += 1 # Increment counter for the next file's unique name part

                # --- Stage 2: Rename File in Processing Folder ---
                operation_stage = "Rename File"
                new_processing_path = os.path.join(proc, new_filename)
                # Check if the file still exists before renaming
                if not os.path.exists(processing_path):
                    raise FileNotFoundError(f"File '{original_filename}' disappeared from processing folder before renaming.")
                os.rename(processing_path, new_processing_path)
                self.log_message(f"Renamed '{original_filename}' to '{new_filename}' in processing folder.")

                # --- Stage 3: Parse the Renamed File ---
                operation_stage = "Parse File"
                # Call the parsing function (assumed to raise Exception on failure)
                parse_file(new_processing_path)
                self.log_message(f"Successfully parsed '{new_filename}'.")

                # --- Stage 4: Move to Processed Folder ---
                operation_stage = "Move to Processed"
                processed_dest_path = os.path.join(pced, new_filename)
                shutil.move(new_processing_path, processed_dest_path)
                self.log_message(f"Moved '{new_filename}' to processed folder: {pced}")

                # Store paths for potential later use (FTP, Secure move)
                path_in_processed = processed_dest_path
                path_in_secured = os.path.join(sec, new_filename)

                # --- Stage 5: Post-Processing Actions (Instructions, FTP) ---
                operation_stage = "Post-Processing Actions"
                # Update progress bar (scheduled on main thread)
                post_progress_val = 80 + (i / total_moved) * 20 # Post-processing is ~20%
                post_progress_text = f"Post-processing ({i + 1}/{total_moved})"
                self.master.after(0, self.update_progress, post_progress_val, post_progress_text, new_filename)

                # Execute actions from matched instruction (if any)
                if matched_instruction:
                    # Launch External EXE (scheduled on main thread)
                    if matched_instruction.get('open_exe'):
                         exe_path_to_launch = matched_instruction['open_exe']
                         self.master.after(0, self._launch_exe, exe_path_to_launch)
                    # Send Custom Email Notification (scheduled on main thread)
                    if matched_instruction.get('send_email'):
                        email_subject = matched_instruction.get('email_subject', 'EDI File Processed (Instruction)')
                        email_body_template = matched_instruction.get('email_body', 'File processed: {new}')
                        # Replace placeholders in body template
                        email_body = email_body_template.replace("{orig}", original_filename).replace("{new}", new_filename)
                        self.master.after(0, self.send_notification_email, email_subject, email_body)

                # --- Stage 6: FTP Upload ---
                # Check if FTP is configured for the effective settings
                if ftp_details.get('ftp_host') and ftp_details.get('ftp_user'):
                     operation_stage = "FTP Upload"
                     self.log_message(f"Attempting FTP upload for '{new_filename}'...")
                     # Run FTP upload directly in this worker thread
                     upload_successful = self._upload_file_ftp(path_in_processed, ftp_details)

                     if upload_successful:
                         self.log_message(f"FTP upload successful for '{new_filename}'.")
                     else:
                         self.log_message(f"FTP upload FAILED for '{new_filename}'.", "ERROR")
                         # Send notification for FTP failure if enabled
                         if notif_settings.get('send_on_error'):
                             details = {
                                 "operation": operation_stage,
                                 "original_filename": original_filename,
                                 "new_filename": new_filename,
                                 "error": "FTP Upload Failed (see logs for details)",
                                 "host": ftp_details.get('ftp_host'),
                                 "remote_path": ftp_details.get('ftp_remote_path')
                             }
                             subject = notif_settings.get('subject_error', 'EDI Process Error')
                             body = self._build_email_body('error', details, config_override=cfg)
                             self.master.after(0, self.send_notification_email, subject, body)
                         # NOTE: Decide if FTP failure should count as overall failure.
                         # Currently, it logs error but continues to Secured move.

                # --- Stage 7: Move to Secured Folder (Final Success Step) ---
                operation_stage = "Move to Secured"
                shutil.move(path_in_processed, path_in_secured)
                self.log_message(f"Moved '{new_filename}' to secured folder.")
                s_c += 1 # Increment success count ONLY after reaching secured folder

            except Exception as e: # Catch ANY exception during this file's processing stages
                f_c += 1 # Increment failure count
                error_message = f"ERROR processing file '{original_filename}' (Stage: {operation_stage}): {e}"
                self.log_message(error_message, "ERROR")
                # Optionally log full traceback for debugging
                # self.log_message(traceback.format_exc(), "DEBUG")

                # --- Error Handling: Move to Error Folder ---
                # Determine the filename to use in the error folder
                error_filename = new_filename if operation_stage not in ["Start Processing", "Check Instructions"] else original_filename
                error_dest_path = os.path.join(err_fld, error_filename)

                # Determine which file path currently exists (original or renamed in processing folder)
                current_file_path_in_proc = new_processing_path if new_processing_path and os.path.exists(new_processing_path) else processing_path

                error_details = { # Prepare details for notification
                    "operation": operation_stage,
                    "original_filename": original_filename,
                    "new_filename": new_filename if operation_stage not in ["Start Processing", "Check Instructions"] else None,
                    "error": str(e),
                    "traceback": traceback.format_exc() # Include traceback
                }

                # Attempt to move the failed file from processing to the error folder
                if os.path.exists(current_file_path_in_proc):
                    try:
                        shutil.move(current_file_path_in_proc, error_dest_path)
                        self.log_message(f"Moved errored file '{os.path.basename(current_file_path_in_proc)}' to error folder.")
                    except Exception as move_err:
                        # Log critical error if move to error folder fails
                        self.log_message(f"CRITICAL ERROR: Failed to move errored file '{os.path.basename(current_file_path_in_proc)}' to error folder: {move_err}", "ERROR")
                        error_details["move_to_error_failed"] = str(move_err)
                else:
                     # Log warning if the file disappeared before it could be moved to error
                     self.log_message(f"Could not find file '{os.path.basename(current_file_path_in_proc)}' in processing folder to move to error folder.", "WARN")
                     error_details["file_missing_for_error_move"] = True

                # --- Send Error Notification (if enabled) ---
                if notif_settings.get('send_on_error'):
                     subject = notif_settings.get('subject_error', 'EDI Process Error')
                     body = self._build_email_body('error', error_details, config_override=cfg)
                     self.master.after(0, self.send_notification_email, subject, body)

            # --- End of try/except block for processing one file ---
        # --- End of loop processing all moved files ---

        # --- Final Cleanup for the Batch ---
        self.master.after(0, self.update_progress, 100, "Batch complete.", "N/A")
        # Close progress window shortly after completion
        self.master.after(100, self.close_progress_window)
        self.log_message(f"Processing batch finished. Results: Success={s_c}, Failed={f_c}, Skipped={k_c}")

        # Return the counts for this batch
        return s_c, f_c, k_c


    # --- File Launching & Upload Methods ---

    def _launch_exe(self, exe_path: str):
         """Launches an external executable specified in instructions (runs via master.after)."""
         if not exe_path:
             self.log_message("Instruction requested launching an empty EXE path.", "WARN")
             return
         # Basic check if path exists and is a file
         if not os.path.isfile(exe_path):
             self.log_message(f"Cannot launch EXE: Path not found or is not a file: '{exe_path}'", "WARN")
             # Optionally send notification about invalid path?
             return

         try:
             self.log_message(f"Launching external executable: {exe_path}")
             # Use Popen for non-blocking execution, ensure shell=False for security
             process = subprocess.Popen([exe_path], shell=False)
             self.log_message(f"Successfully launched '{os.path.basename(exe_path)}' (PID: {process.pid})")
         except Exception as e:
             self.log_message(f"Failed to launch executable '{exe_path}': {e}", "ERROR")
             # Send notification on launch failure
             try:
                 active_settings = self._get_active_settings()
                 notif_cfg = active_settings.get('notification_settings', {})
                 if notif_cfg.get('send_on_error'):
                     details = {"operation": "Launch Executable", "executable_path": exe_path, "error": str(e)}
                     subject = notif_cfg.get('subject_error', 'EDI Process Error')
                     body = self._build_email_body('error', details, config_override=active_settings)
                     # Already in main thread context from master.after, call directly
                     self.send_notification_email(subject, body)
             except Exception as notif_err:
                  self.log_message(f"Failed to send launch error notification: {notif_err}", "ERROR")


    def _upload_file_ftp(self, local_file_path: str, ftp_details: Dict) -> bool:
        """
        Uploads a single file via FTP. Executed directly within the processing thread.

        Args:
            local_file_path: Full path to the local file.
            ftp_details: Dictionary containing FTP parameters (host, port, user, etc.).

        Returns:
            True if upload was successful, False otherwise.
        """
        host = ftp_details.get('ftp_host')
        port = ftp_details.get('ftp_port', 21)
        user = ftp_details.get('ftp_user')
        password = ftp_details.get('ftp_password', '') # Default to empty string if None/missing
        remote_path = ftp_details.get('ftp_remote_path', '/')

        # Validate essential parameters
        if not host or not user:
            self.log_message(f"FTP Skipped: Host or Username missing for client '{ftp_details.get('_client_name', 'General')}'", "WARN")
            return False

        # Ensure remote path format (ends with / for directory)
        if not remote_path.endswith('/'): remote_path += '/'
        remote_filename = os.path.basename(local_file_path)
        full_remote_target_path = remote_path + remote_filename

        ftp = None # Initialize for finally block
        try:
            self.log_message(f"FTP Connecting to {host}:{port} as user '{user}'...")
            ftp = ftplib.FTP()
            ftp.connect(host, port, timeout=30) # Connection timeout
            ftp.login(user, password)
            self.log_message("FTP connection successful.")
            ftp.set_pasv(True) # Use passive mode
            self.log_message("FTP passive mode enabled.")

            # Attempt to change to remote directory, create if needed (optional)
            try:
                 ftp.cwd(remote_path)
                 self.log_message(f"FTP changed to remote directory: {remote_path}")
            except ftplib.error_perm as e:
                 if "550" in str(e): # Directory likely doesn't exist
                      self.log_message(f"FTP remote path '{remote_path}' not found. Attempting to create...", "WARN")
                      try:
                           ftp.mkd(remote_path)
                           self.log_message(f"FTP created remote directory: {remote_path}")
                           ftp.cwd(remote_path) # Change into newly created directory
                      except ftplib.all_errors as mkd_e:
                           self.log_message(f"FTP Error: Failed to create/access remote dir '{remote_path}': {mkd_e}", "ERROR")
                           return False # Cannot proceed
                 else: # Other permission error
                      self.log_message(f"FTP Error: Could not change to remote dir '{remote_path}': {e}", "ERROR")
                      return False

            # Upload the file
            self.log_message(f"FTP Uploading '{local_file_path}' to '{remote_filename}' in '{remote_path}'...")
            with open(local_file_path, 'rb') as file_handle:
                ftp.storbinary(f'STOR {remote_filename}', file_handle) # Upload to current remote dir

            self.log_message(f"FTP upload successful for '{remote_filename}'.")
            return True

        except ftplib.all_errors as e:
            self.log_message(f"FTP Error during operation for '{remote_filename}': {e}", "ERROR")
            return False
        except socket.gaierror as e:
             self.log_message(f"FTP Error: Could not resolve host '{host}': {e}", "ERROR")
             return False
        except socket.timeout:
             self.log_message(f"FTP Error: Connection to '{host}:{port}' timed out.", "ERROR")
             return False
        except Exception as e:
            self.log_message(f"FTP Unexpected Error during upload of '{remote_filename}': {e}", "ERROR")
            self.log_message(traceback.format_exc(), "DEBUG")
            return False
        finally:
            # Ensure connection is closed
            if ftp:
                try:
                    ftp.quit()
                    self.log_message("FTP connection closed.")
                except Exception:
                    pass # Ignore errors during quit


    # --- Settings Window Launchers ---
    def open_general_settings(self):
        """Opens the General Settings configuration window."""
        try:
            # Pass the main config dict and the global save function
            win = GeneralConfigWindow(self.master, self.config, save_configuration)
            self.master.wait_window(win) # Wait for the settings window to close
            self.log_message("General Settings window closed. Reloading configuration.")
            # Reload config in case changes were made
            self.config = load_configuration()
            self.update_active_client_display() # Update display based on potentially changed config
        except Exception as e:
            self.log_message(f"Error opening General Settings window: {e}", "ERROR")
            messagebox.showerror("GUI Error", "Could not open General Settings window. Check logs.")

    def open_client_profiles(self):
         """Opens the Client Profiles management window."""
         try:
             # Pass the global save function and the instance's log function
             win = ClientConfigWindow(self.master, save_configuration, self.log_message)
             self.master.wait_window(win) # Wait for the window to close
             self.log_message("Client Profiles window closed. Reloading configuration.")
             # Reload config as active client or profiles might have changed
             self.config = load_configuration()
             self.update_active_client_display() # Update title/status bar
         except Exception as e:
             self.log_message(f"Error opening Client Profiles window: {e}", "ERROR")
             messagebox.showerror("GUI Error", "Could not open Client Profiles window. Check logs.")

    def open_notification_settings(self):
        """Opens the Notification Settings configuration window."""
        try:
            # Pass main config and global save function
            win = NotificationConfigWindow(self.master, self.config, save_configuration)
            self.master.wait_window(win)
            # Reload config to ensure consistency after potential save
            self.config = load_configuration()
            self.log_message("Notification Settings window closed.")
        except Exception as e:
            self.log_message(f"Error opening Notification Settings window: {e}", "ERROR")
            messagebox.showerror("Error", "Could not open Notification Settings window. Check logs.")


    # --- Special Instructions Window Methods ---
    def edit_special_instructions(self):
        """Opens the window for editing global special instructions."""
        # Define column weights for layout (could be class attribute)
        self.instr_col_weights = [1, 3, 4, 5, 1, 4, 6] # Adjust as needed

        # Prevent opening multiple instances
        if hasattr(self, 'edit_instructions_window') and self.edit_instructions_window and tk.Toplevel.winfo_exists(self.edit_instructions_window):
            self.edit_instructions_window.lift() # Bring existing window to front
            return

        try:
            # --- Create Toplevel Window ---
            self.edit_instructions_window = tk.Toplevel(self.master)
            self.edit_instructions_window.title("Edit Special Instructions (Global Rules)")
            self.edit_instructions_window.geometry("1250x500") # Wide window
            self.edit_instructions_window.transient(self.master)
            self.edit_instructions_window.protocol("WM_DELETE_WINDOW", self._on_edit_instructions_close) # Handle close
            self.edit_instructions_window.grab_set() # Make modal

            # Configure main window grid (1 column, row 1 expands)
            self.edit_instructions_window.grid_columnconfigure(0, weight=1)
            self.edit_instructions_window.grid_rowconfigure(1, weight=1)

            # --- Header Row ---
            header_frame = ttk.Frame(self.edit_instructions_window)
            header_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 0))
            headers = ["Enabled", "Filename Contains", "New Filename Base", "Open EXE Path", "Send Email?", "Email Subject", "Email Body Template"]
            for col, (text, weight) in enumerate(zip(headers, self.instr_col_weights)):
                lbl = ttk.Label(header_frame, text=text, relief=tk.GROOVE, anchor=tk.CENTER, padding=(2, 2))
                lbl.grid(row=0, column=col, sticky="nsew", padx=1, pady=1)
                header_frame.grid_columnconfigure(col, weight=weight) # Apply column weights

            # --- Scrollable Frame for Instruction Rows ---
            canvas_frame = ttk.Frame(self.edit_instructions_window)
            canvas_frame.grid(row=1, column=0, sticky="nsew", padx=10)
            canvas_frame.grid_rowconfigure(0, weight=1) # Canvas expands vertically
            canvas_frame.grid_columnconfigure(0, weight=1) # Canvas expands horizontally

            canvas = tk.Canvas(canvas_frame)
            scrollbar = ttk.Scrollbar(canvas_frame, orient=tk.VERTICAL, command=canvas.yview)
            # This frame holds the actual instruction rows and is placed inside the canvas
            self.scrollable_instruction_frame = ttk.Frame(canvas)

            # Link scrollbar and canvas
            self.scrollable_instruction_frame.bind(
                "<Configure>", # Reconfigure scroll region when frame size changes
                lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
            )
            canvas.create_window((0, 0), window=self.scrollable_instruction_frame, anchor="nw")
            canvas.configure(yscrollcommand=scrollbar.set)

            # Pack canvas and scrollbar into their container frame
            canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

            # --- Populate Rows ---
            self.instruction_vars = [] # List to hold tuples of tk variables for each row
            self._populate_instruction_rows() # Call helper to create rows

            # --- Bottom Buttons ---
            button_frame = ttk.Frame(self.edit_instructions_window)
            button_frame.grid(row=2, column=0, sticky="ew", padx=10, pady=10)
            ttk.Button(button_frame, text="Add New Rule", command=self.add_instruction_row).pack(side=tk.LEFT, padx=5)
            ttk.Button(button_frame, text="Save Changes", command=self.save_instructions).pack(side=tk.RIGHT, padx=5)
            ttk.Button(button_frame, text="Cancel", command=self._on_edit_instructions_close).pack(side=tk.RIGHT, padx=5)

        except Exception as e:
            self.log_message(f"Error opening Special Instructions window: {e}", "ERROR")
            messagebox.showerror("GUI Error", "Could not open Special Instructions window.")
            # Clean up window reference if creation failed partially
            if hasattr(self, 'edit_instructions_window') and self.edit_instructions_window:
                 try: self.edit_instructions_window.destroy()
                 except: pass
            self.edit_instructions_window = None

    def _populate_instruction_rows(self):
         """Clears and rebuilds the rows in the special instructions editor."""
         # Clear existing widgets in the scrollable frame
         for widget in self.scrollable_instruction_frame.winfo_children():
             widget.destroy()
         self.instruction_vars.clear() # Clear the list of variable tuples

         # Get current instructions, ensure it's a list
         current_instructions = self.config.get('special_instructions', [])
         if not isinstance(current_instructions, list):
              self.log_message("Warning: 'special_instructions' in config is not a list. Resetting.", "WARN")
              current_instructions = []
              self.config['special_instructions'] = current_instructions

         # Define approximate widths for entry fields (adjust as needed)
         w_contains = 18; w_filename = 25; w_exe = 30; w_subject = 30; w_body = 40

         # Create a row frame and widgets for each instruction
         for idx, instruction_data in enumerate(current_instructions):
             # Ensure the instruction dict has all default keys
             instruction = {**DEFAULT_SPECIAL_INSTRUCTION, **(instruction_data if isinstance(instruction_data, dict) else {})}

             # Create a frame for this row within the scrollable frame
             row_frame = ttk.Frame(self.scrollable_instruction_frame)
             row_frame.grid(row=idx, column=0, sticky="ew", pady=1, padx=1)
             # Apply column weights to the row frame for consistent layout
             for col, weight in enumerate(self.instr_col_weights):
                 row_frame.grid_columnconfigure(col, weight=weight)

             # Create Tkinter variables for this row's data
             en_var = tk.BooleanVar(value=instruction["enabled"])
             con_var = tk.StringVar(value=instruction["contains"])
             fn_var = tk.StringVar(value=instruction["new_filename"])
             exe_var = tk.StringVar(value=instruction["open_exe"])
             mail_var = tk.BooleanVar(value=instruction["send_email"])
             subj_var = tk.StringVar(value=instruction["email_subject"])
             body_var = tk.StringVar(value=instruction["email_body"])

             # Store the variables tuple for saving later
             self.instruction_vars.append((en_var, con_var, fn_var, exe_var, mail_var, subj_var, body_var))

             # --- Create and Grid Widgets for the row ---
             ttk.Checkbutton(row_frame, variable=en_var).grid(row=0, column=0, padx=(0, 5), sticky='w')
             ttk.Entry(row_frame, textvariable=con_var, width=w_contains).grid(row=0, column=1, padx=5, sticky='ew')
             ttk.Entry(row_frame, textvariable=fn_var, width=w_filename).grid(row=0, column=2, padx=5, sticky='ew')
             # Frame for EXE path entry + browse button
             exe_subframe = ttk.Frame(row_frame)
             exe_subframe.grid(row=0, column=3, padx=5, sticky='ew')
             exe_subframe.columnconfigure(0, weight=1) # Make entry expand
             ttk.Entry(exe_subframe, textvariable=exe_var, width=w_exe).grid(row=0, column=0, sticky='ew')
             ttk.Button(exe_subframe, text="...", width=3, command=lambda v=exe_var: self._browse_exe(v)).grid(row=0, column=1, padx=(2,0))
             # Email related widgets
             ttk.Checkbutton(row_frame, variable=mail_var).grid(row=0, column=4, padx=5, sticky='w')
             ttk.Entry(row_frame, textvariable=subj_var, width=w_subject).grid(row=0, column=5, padx=5, sticky='ew')
             ttk.Entry(row_frame, textvariable=body_var, width=w_body).grid(row=0, column=6, padx=(5, 0), sticky='ew')

    def _browse_exe(self, string_var: tk.StringVar):
        """Opens a file dialog to select an executable file for an instruction."""
        # Ensure the parent window exists
        if not (hasattr(self, 'edit_instructions_window') and self.edit_instructions_window and tk.Toplevel.winfo_exists(self.edit_instructions_window)):
             self.log_message("Cannot browse for EXE: Instructions window not available.", "WARN")
             return
        # Define file types for executable selection
        filetypes = (("Executable files", "*.exe"), ("All files", "*.*"))
        exe_path = filedialog.askopenfilename(
            title="Select Executable File",
            filetypes=filetypes,
            parent=self.edit_instructions_window # Set parent to modal window
        )
        # Update the StringVar if a path was selected
        if exe_path:
            string_var.set(exe_path)

    def add_instruction_row(self):
        """Adds a new empty instruction rule to the config and refreshes the GUI."""
        # Ensure 'special_instructions' exists and is a list
        if not isinstance(self.config.get('special_instructions'), list):
            self.config['special_instructions'] = []
        # Append a new default instruction dictionary
        self.config['special_instructions'].append(DEFAULT_SPECIAL_INSTRUCTION.copy())
        # Refresh the GUI rows to show the new empty row
        self._populate_instruction_rows()
        # Optionally scroll to the bottom (requires canvas reference)
        # if hasattr(self, 'canvas'): self.canvas.yview_moveto(1.0)

    def save_instructions(self):
        """Saves the current state of the instruction rules back to the config file."""
        updated_instructions = []
        try:
            # Iterate through the stored variable tuples for each row
            for row_vars in self.instruction_vars:
                # Basic check for expected tuple length
                if len(row_vars) == len(DEFAULT_SPECIAL_INSTRUCTION):
                    en_var, con_var, fn_var, exe_var, mail_var, subj_var, body_var = row_vars
                    # Create a dictionary from the current variable values
                    instruction_dict = {
                        "enabled": en_var.get(),
                        "contains": con_var.get().strip(),
                        "new_filename": fn_var.get().strip(),
                        "open_exe": exe_var.get().strip(),
                        "send_email": mail_var.get(),
                        "email_subject": subj_var.get().strip(),
                        "email_body": body_var.get() # Don't strip body template
                    }
                    updated_instructions.append(instruction_dict)
                else:
                    self.log_message(f"Internal Error: Found instruction variable tuple with unexpected length {len(row_vars)}. Skipping.", "ERROR")

            # Update the main configuration dictionary
            self.config['special_instructions'] = updated_instructions

            # Call the global save function
            if save_configuration(self.config):
                self.log_message("Special instructions saved successfully.")
                messagebox.showinfo("Saved", "Special instructions saved.", parent=self.edit_instructions_window)
                self._on_edit_instructions_close() # Close the window on success
            else:
                messagebox.showerror("Save Error", "Failed to save special instructions. Check logs.", parent=self.edit_instructions_window)

        except Exception as e:
            self.log_message(f"Error saving special instructions: {e}", "ERROR")
            messagebox.showerror("Error", f"An unexpected error occurred while saving instructions:\n{e}", parent=self.edit_instructions_window)

    def _on_edit_instructions_close(self):
        """Handles closing the special instructions window cleanly."""
        if hasattr(self, 'edit_instructions_window') and self.edit_instructions_window:
            try:
                 if tk.Toplevel.winfo_exists(self.edit_instructions_window):
                     self.edit_instructions_window.grab_release() # Release modal grab
                     self.edit_instructions_window.destroy() # Destroy window
            except Exception as e:
                 self.log_message(f"Error closing instructions window: {e}", "WARN")
            finally:
                 self.edit_instructions_window = None # Clear reference


    # --- Email & State Persistence Methods ---
    def _build_email_body(self, event_type: str, details: Dict, config_override: Optional[Dict] = None) -> str:
        """Constructs the email body based on event type and details."""
        cfg = config_override if config_override is not None else self._get_active_settings()
        notif = cfg.get('notification_settings', {})
        body_lines = ["EDI Process Notification", "="*30]

        # Timestamp
        if notif.get('include_timestamp', True):
            body_lines.append(f"Time: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}")

        # Client Context
        client_name = cfg.get('_client_name') # Added by _get_active_settings
        body_lines.append(f"Client Profile: {client_name if client_name else 'General Settings'}")
        body_lines.append(f"Event Type: {event_type.upper()}")

        # Error Details
        if event_type == 'error':
            body_lines.append("\n--- ERROR DETAILS ---")
            body_lines.append(f"Context/Operation: {details.get('operation', details.get('context', 'N/A'))}")
            if notif.get('include_original_filename', True) and details.get('original_filename'):
                 body_lines.append(f"Original Filename: {details['original_filename']}")
            if notif.get('include_new_filename', True) and details.get('new_filename'):
                 body_lines.append(f"New Filename: {details['new_filename']}")
            # Include specific paths if available in details
            if details.get('executable_path'): body_lines.append(f"Executable Path: {details['executable_path']}")
            if details.get('host'): body_lines.append(f"FTP Host: {details['host']}")
            if details.get('source'): body_lines.append(f"Source Path: {details['source']}")
            if details.get('destination'): body_lines.append(f"Destination Path: {details['destination']}")

            # Include error message and potentially traceback
            if notif.get('include_error_details', True):
                body_lines.append(f"\nError Message:\n{details.get('error', 'N/A')}")
                # Optionally include traceback only if available and needed
                if details.get('traceback'):
                     body_lines.append(f"\nTraceback Snippet:\n{details['traceback'].splitlines()[-1]}") # Example: last line

            # Include specific failure flags if present
            if details.get('move_to_error_failed'): body_lines.append(f"\nCRITICAL: Failed to move file to error folder: {details['move_to_error_failed']}")
            if details.get('file_missing_for_error_move'): body_lines.append("\nWARNING: Original file could not be found to move to error folder.")

        # Summary Details
        elif event_type == 'summary':
            body_lines.append("\n--- PROCESSING SUMMARY ---")
            body_lines.append(f"Context: {details.get('context', 'Batch Processing')}")
            if notif.get('include_success_fail_counts', True):
                 body_lines.append(f"Files Processed Successfully: {details.get('success_count', 'N/A')}")
                 body_lines.append(f"Files Failed: {details.get('fail_count', 'N/A')}")
                 body_lines.append(f"Files Skipped/Ignored: {details.get('skipped_count', 'N/A')}")

        # Fallback for unknown types
        else:
            body_lines.append(f"\nDetails: {details}")

        body_lines.append("\n" + "="*30)
        return "\n".join(body_lines)

    def send_notification_email(self, subject: str, body: str):
        """Simulates sending an email by printing to console/log."""
        # Get recipient from the current effective configuration
        active_settings = self._get_active_settings()
        recipient = active_settings.get('notification_settings', {}).get('recipient_email')

        if not recipient:
            self.log_message("Email notification skipped: Recipient email address is not configured.", "WARN")
            return

        # --- Email Simulation ---
        email_output = [
            "\n--- Email Simulation ---", f"To: {recipient}", f"Subject: {subject}",
            "--- Body ---", body, "--- End Email ---"
        ]
        log_msg = f"Simulated email sent to '{recipient}' with subject: '{subject}'"
        print("\n".join(email_output)) # Print simulation to console
        self.log_message(log_msg) # Log the simulation action
        # --- End Simulation ---

        # TODO: Replace simulation with actual email sending logic using smtplib or another library.
        # Requires configuring sender email, password/token, SMTP server, port.

    def save_schedule_state(self, is_running: bool):
        """Saves the current running state of the scheduler to STATE_FILE."""
        state_data = {"scheduled_processing_running": is_running}
        try:
            # Corrected: Use open() within the with statement
            with open(STATE_FILE, "w") as f:
                json.dump(state_data, f)
            self.log_message(f"Saved schedule state: running={is_running}", "DEBUG")
        except Exception as e:
            self.log_message(f"Error saving schedule state to '{STATE_FILE}': {e}", "ERROR")

    def load_schedule_state(self) -> bool:
        """Loads the last known running state of the scheduler from STATE_FILE."""
        if not os.path.exists(STATE_FILE):
            self.log_message(f"Schedule state file '{STATE_FILE}' not found. Assuming not running.", "INFO")
            return False # Default to False if file doesn't exist
        try:
            with open(STATE_FILE, "r") as f:
                state_data = json.load(f)
            is_running = state_data.get("scheduled_processing_running", False)
            self.log_message(f"Loaded schedule state: running={is_running}", "DEBUG")
            return is_running
        except json.JSONDecodeError as e:
             self.log_message(f"Error decoding schedule state file '{STATE_FILE}': {e}. Assuming not running.", "ERROR")
             return False
        except Exception as e:
            self.log_message(f"Error loading schedule state from '{STATE_FILE}': {e}. Assuming not running.", "ERROR")
            return False


    # --- Configuration Update & Validation Methods ---
    def _update_config_from_gui(self) -> bool:
        """Updates the main config dict with values from GUI widgets (schedule interval, auto-restart)."""
        try:
            interval = self.interval_var.get()
            auto_restart = self.auto_restart_var.get()

            # Basic validation for interval
            if not isinstance(interval, int) or interval <= 0:
                 self.log_message(f"Invalid interval value '{interval}' in GUI. Using default.", "WARN")
                 interval = DEFAULT_CONFIG['schedule_interval_minutes']
                 self.interval_var.set(interval) # Correct GUI variable too

            # Update the config dictionary
            self.config['schedule_interval_minutes'] = interval
            self.config['auto_restart_schedule'] = auto_restart
            return True # Indicate success

        except tk.TclError as e:
            # Handle potential errors reading from tk variables (e.g., if window is closing)
            self.log_message(f"Error reading schedule settings from GUI: {e}", "WARN")
            return False
        except Exception as e:
             self.log_message(f"Unexpected error updating config from GUI: {e}", "ERROR")
             return False

    def save_settings_to_config(self):
         """Saves schedule settings from the GUI to the config file."""
         self.log_message("Attempting to save schedule settings from GUI...")
         # First, update the self.config dict from the GUI variables
         if self._update_config_from_gui():
              # If update was successful, call the global save function
              if save_configuration(self.config):
                  self.log_message("Schedule interval and auto-restart settings saved successfully.")
                  messagebox.showinfo("Saved", "Schedule settings saved.", parent=self.master)
              else:
                  # Error message shown by save_configuration
                  self.log_message("Failed to save configuration file with updated schedule settings.", "ERROR")
         else:
              # Update from GUI failed
              self.log_message("Could not save schedule settings due to error reading values from GUI.", "ERROR")
              messagebox.showerror("Error", "Could not read settings from GUI. Save aborted.", parent=self.master)

    def _get_active_settings(self, base_config: Optional[Dict] = None) -> Dict:
        """
        Determines the effective configuration settings to use based on the
        active client profile, falling back to general settings otherwise.

        Args:
            base_config: Optional config dictionary to use instead of self.config.

        Returns:
            A dictionary containing the merged settings. Includes a '_client_name'
            key indicating the active profile name (str) or None.
        """
        current_base = base_config if base_config is not None else self.config
        active_client_name = current_base.get('active_client')
        client_profiles = current_base.get('clients', {})

        # Start with a copy of the base config, excluding client-specific structural keys
        effective_settings = {k: v for k, v in current_base.items() if k not in ['clients', 'active_client']}
        effective_settings['_client_name'] = None # Initialize marker

        # If a client is active and exists in profiles, override general settings
        if active_client_name and active_client_name in client_profiles:
            profile_data = client_profiles[active_client_name]
            self.log_message(f"Using profile for active client: '{active_client_name}'", "DEBUG")
            effective_settings['_client_name'] = active_client_name

            # Override general settings with client profile settings
            # Use profile value even if it's an empty string (allows clearing a general setting)
            for key in DEFAULT_CLIENT_PROFILE.keys():
                 if key in profile_data: # Check if key exists in the loaded profile
                     effective_settings[key] = profile_data[key]
                 # else: Keep the value from the general config (already in effective_settings)

        else:
             # Log if active client is set but not found, or if no client is active
             if active_client_name:
                  self.log_message(f"Active client '{active_client_name}' not found in profiles. Using general settings.", "WARN")
             else:
                  self.log_message("No active client selected. Using general settings.", "DEBUG")
            # No client active or profile missing, effective_settings already holds general config.

        # Ensure essential nested structures exist in the effective settings
        effective_settings.setdefault('notification_settings', DEFAULT_NOTIFICATION_SETTINGS.copy())
        effective_settings.setdefault('special_instructions', []) # Use empty list if missing

        return effective_settings

    def _validate_folder_paths(self, config_dict: Dict, keys_to_check: list) -> bool:
         """Checks if specified folder path keys in the config dict point to non-empty strings."""
         all_valid = True
         for key in keys_to_check:
             path = config_dict.get(key)
             # Check if path is missing, None, or an empty/whitespace-only string
             if not path or not isinstance(path, str) or not path.strip():
                  self.log_message(f"Validation Error: Folder path for '{key}' is missing or invalid in the configuration.", "ERROR")
                  all_valid = False
         return all_valid

    def _validate_active_config(self, active_settings: Dict, log_prefix: str = "") -> bool:
         """Validates essential settings (like folder paths) needed for processing."""
         client_name = active_settings.get('_client_name', 'General')
         prefix = f"{log_prefix}: " if log_prefix else ""

         # --- Check Essential Folders ---
         required_folders = ["source_folder", "processing_folder", "processed_folder", "secured_folder", "error_folder"]
         if not self._validate_folder_paths(active_settings, required_folders):
              # Detailed error message for user
              messagebox.showerror("Configuration Error",
                                   f"{prefix}One or more essential folder paths (Source, Processing, Processed, Secured, Error) "
                                   f"are missing or invalid for '{client_name}' settings.\n\n"
                                   "Please check configuration under Settings or Client Profiles.",
                                   parent=self.master)
              return False # Validation failed

         # --- Check EDI Extension ---
         edi_ext = active_settings.get('edi_extension')
         if not edi_ext or not isinstance(edi_ext, str) or not edi_ext.startswith('.'):
              self.log_message(f"{prefix}Invalid EDI extension '{edi_ext}' for '{client_name}'. Must be a string starting with '.'. Using default '.edi'.", "WARN")
              # Correct it temporarily in the settings being used for this run
              active_settings['edi_extension'] = '.edi'
              # Note: This does not save the correction back to config.json automatically

         # --- Add more checks as needed ---
         # Example: Check FTP details if FTP upload is expected/required based on other settings

         self.log_message(f"{prefix}Configuration validated successfully for '{client_name}'.", "DEBUG")
         return True # All checks passed


    # --- Display Update Methods ---
    def update_active_client_display(self):
         """Updates the window title and status bar with the active client name."""
         try:
             active_client = self.config.get('active_client', 'None Selected')
             title = f"EDI Processor - Client: [{active_client}]"
             # Check if master window still exists before setting title
             if self.master.winfo_exists():
                 self.master.title(title)
             # Update status bar text
             self.update_status(f"Active Client: {active_client}")
         except Exception as e:
              print(f"Error updating active client display: {e}")

    def update_status(self, message: str):
        """Updates the text in the status bar label."""
        # Check if status label exists and its window is valid
        if hasattr(self, 'status_label') and self.status_label.winfo_exists():
             try:
                 self.status_label.config(text=f"Status: {message}")
             except tk.TclError as e:
                  # Ignore errors typically occurring during shutdown
                  print(f"WARN: Error updating status bar (likely during shutdown): {e}")
        # else: Status bar doesn't exist or was destroyed


# --- END OF EDIProcessorGUI CLASS DEFINITION ---


# ==========================================================
# Main Execution Block
# ==========================================================
if __name__ == "__main__":
    # --- Setup Logging (Basic Console Logging) ---
    # You might want to configure file logging here as well
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # --- Initialize Tkinter ---
    root = tk.Tk()

    # --- Apply Theming (Optional but Recommended) ---
    try:
        style = ttk.Style(root)
        available_themes = style.theme_names()
        # print(f"Available themes: {available_themes}") # Uncomment to see available themes
        # Try to use a modern theme if available
        if 'clam' in available_themes: style.theme_use('clam')
        elif 'vista' in available_themes: style.theme_use('vista') # Windows
        elif 'aqua' in available_themes: style.theme_use('aqua')   # macOS
        # Add other preferences or fallbacks as needed
    except Exception as e:
        print(f"Theme Error: Could not set preferred theme: {e}")

    # --- Create and Run Application Instance ---
    app = EDIProcessorGUI(root)

    # --- Graceful Shutdown Handling ---
    def on_closing():
        """Handles the window close event (clicking the [X] button)."""
        app_instance = app # Reference the app instance

        # Check if scheduled processing is running
        if app_instance.scheduled_processing_running:
            if messagebox.askokcancel("Quit", "Scheduled processing is active.\nStop schedule and quit the application?", parent=root):
                print("INFO: Stopping schedule due to application close...")
                app_instance.stop_scheduled_processing() # Signal stop
                # Save current settings (like interval) before exiting
                app_instance.save_settings_to_config()
                # Give a moment for the stop signal to be processed if needed
                # and destroy the main window
                root.after(200, root.destroy)
            else:
                return # Don't close if user cancels

        # Check if manual processing is running
        elif app_instance._manual_processing_active:
             if messagebox.askokcancel("Quit", "Manual processing is active.\nQuit the application anyway? (Processing might not complete cleanly)", parent=root):
                 print("INFO: Quitting during manual processing...")
                 # Manual run thread is daemon, should exit with app, but state might be inconsistent
                 # Save current settings before exiting
                 app_instance.save_settings_to_config()
                 root.destroy()
             else:
                  return # Don't close

        # If neither process is running, confirm quit
        else:
            if messagebox.askokcancel("Quit", "Are you sure you want to quit?", parent=root):
                 print("INFO: Quitting application...")
                 # Save current settings before exiting
                 app_instance.save_settings_to_config()
                 root.destroy() # Close immediately
            # else: Do nothing if user cancels quit confirmation

    # Bind the close handler to the window's close button protocol
    root.protocol("WM_DELETE_WINDOW", on_closing)

    # Start the Tkinter main event loop
    root.mainloop()

    print("INFO: Application finished.")