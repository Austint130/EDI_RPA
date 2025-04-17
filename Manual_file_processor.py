import datetime
import os
import shutil
import tkinter as tk
from tkinter import ttk, messagebox, filedialog, simpledialog
from zipfile import ZipFile, ZIP_DEFLATED

import requests
from cryptography.fernet import Fernet  # type: ignore


# --- New EDI File Generation Code ---
# Mock EDI generation functionality
class Segment:
    def __init__(self, segment_id, elements):
        self.segment_id = segment_id
        self.elements = elements

    def __str__(self):
        return f"{self.segment_id}*{'*'.join(self.elements)}"

class Message:
    def __init__(self, message_type, version, control_number):
        self.message_type = message_type
        self.version = version
        self.control_number = control_number
        self.segments = []

    def add_segment(self, segment):
        self.segments.append(segment)

    def __str__(self):
        return "\n".join(str(segment) for segment in self.segments)

class Interchange:
    def __init__(self, sender, recipient, date, time, control_number):
        self.sender = sender
        self.recipient = recipient
        self.date = date
        self.time = time
        self.control_number = control_number
        self.messages = []

    def add_message(self, message):
        self.messages.append(message)

    def serialize(self):
        return "\n".join(str(message) for message in self.messages)
def generate_mock_edi(client_id, num_files=1):
    """
    Generates mock EDI files for a specified client.

    Args:
        client_id (str): The ID of the client (e.g., "buyer_A", "buyer_B").
        num_files (int): The number of mock EDI files to generate.  Defaults to 1.
    """
    # Define the base directory where EDI files will be created
    base_dir = f"edi_files/{client_id.lower()}"  # Use lower() to match dir names
    os.makedirs(base_dir, exist_ok=True)  # Create the directory if it doesn't exist

    for i in range(num_files):
        # Create a basic EDI interchange (you'll need to adapt this to your needs)
        interchange = Interchange(
            sender=f"Sender{client_id}",  # Sender ID
            recipient=f"Receiver{client_id}",  # Recipient ID
            date=datetime.date.today(),
            time=datetime.datetime.now().time(),
            control_number=i + 1,  # Unique interchange control number
        )

        # Create a message within the interchange
        message = Message(
            message_type="ORDERS",  # Example message type
            version="4010",  # Example version
            control_number=100 + i,  # Unique message control number
        )

        # Add segments to the message (this is where you define the EDI data)
        message.add_segment(Segment("BEG", ["00", "SA", f"Order{i}", str(datetime.date.today())]))  # Example BEG segment
        message.add_segment(Segment("REF", ["PO", f"PO{i}"]))  # Example REF segment
        message.add_segment(Segment("N1", ["ST", f"Supplier{client_id}"]))  # Example N1 segment
        message.add_segment(Segment("N1", ["BY", f"Buyer{client_id}"]))
        message.add_segment(Segment("IT1", ["1", "10", "EA", "100", "20", "USD", "Product1"]))  # Example IT1 segment
        message.add_segment(Segment("IT1", ["2", "10", "EA", "150", "20", "USD", "Product2"]))
        message.add_segment(Segment("CTT", ["2", "200"]))  # Example CTT segment

        interchange.add_message(message)

        # Serialize the interchange to an EDI string
        edi_string = interchange.serialize()

        # Construct the file path
        file_path = os.path.join(base_dir, f"order_{client_id}_{i}.edi")

        # Write the EDI string to a file
        with open(file_path, "w") as f:
            f.write(edi_string)
        print(f"Generated mock EDI file: {file_path}")

def generate_mock_edi_files_for_all():
    """Generates mock EDI files for all three buyers (A, B, and C)."""
    generate_mock_edi("buyer_A", 2)  # Generate 2 files for buyer_A
    generate_mock_edi("buyer_B", 3)  # Generate 3 files for buyer_B
    generate_mock_edi("buyer_C", 1)  # Generate 1 file for buyer_C
    print("Finished generating mock EDI files.")

# --- End New EDI File Generation Code ---

class FileManagerApp:
    def __init__(self):
        # Initialize main window
        self.window = tk.Tk()
        self.window.title("Zoho RPA File Manager Pro")
        self.window.geometry("1200x900")
        self.window.configure(bg='#f0f0f0')

        # Configuration
        self.DEFAULT_PATHS = {
            "shared": "Shared/",
            "backup": "Backup/",
            "staging": "Staging/",
            "processed": "Secured/Processed/",
            "error": "Secured/Errors/",
            "zip": "Zipped/",
            "logs": "Logs/"
        }

        # Webhook configuration
        self.WEBHOOK_URL = "https://rpa.zoho.com/882919207/flow/webhook/incoming"
        self.API_KEY = "1001.3342855f2dfd83728006cec6e6d0afed.5ea29cf8dd0772de446cb5dd1e95e98f"

        # Encryption setup
        self.encryption_key = Fernet.generate_key()
        self.cipher_suite = Fernet(self.encryption_key)

        # GUI variables
        self.current_shared_folder = tk.StringVar(value=self.DEFAULT_PATHS["shared"])
        self.webhook_url = tk.StringVar(value=self.WEBHOOK_URL)

        # Application state
        self.file_data = []
        self.log_file = None
        self.log_file_initialized = False

        # Initialize components
        self.setup_gui()
        self.initialize_logging()
        self.create_folders()
        self.update_file_list()

    def setup_gui(self):
        """Configure GUI components"""
        style = ttk.Style()
        style.theme_use('clam')
        style.configure('TButton', font=('Arial', 10), padding=4)
        style.configure('Title.TLabel', font=('Arial', 14, 'bold'), background='#f0f0f0')

        main_frame = ttk.Frame(self.window, padding=20)
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Webhook configuration
        webhook_frame = ttk.Frame(main_frame)
        webhook_frame.grid(row=0, column=0, pady=5, sticky='ew')

        ttk.Label(webhook_frame, text="Webhook URL:").pack(side=tk.LEFT)
        webhook_entry = ttk.Entry(webhook_frame, textvariable=self.webhook_url, width=100)
        webhook_entry.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)
        ttk.Button(webhook_frame, text="Test", command=self.test_webhook, width=6).pack(side=tk.LEFT)

        # Folder selection
        path_frame = ttk.Frame(main_frame)
        path_frame.grid(row=1, column=0, pady=5, sticky='ew')

        ttk.Label(path_frame, text="Source Folder:").pack(side=tk.LEFT)
        path_entry = ttk.Entry(path_frame, textvariable=self.current_shared_folder, width=80)
        path_entry.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)
        ttk.Button(path_frame, text="Browse...", command=self.browse_shared_folder).pack(side=tk.LEFT)

        # File list
        list_frame = ttk.LabelFrame(main_frame, text=" File Browser ")
        list_frame.grid(row=2, column=0, pady=10, sticky='nsew')

        file_scroll = ttk.Scrollbar(list_frame)
        self.file_listbox = tk.Listbox(
            list_frame,
            yscrollcommand=file_scroll.set,
            font=('Arial', 11),
            selectbackground='#c0e0ff',
            height=18
        )
        file_scroll.config(command=self.file_listbox.yview)
        self.file_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        file_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # Progress bar
        self.progress = ttk.Progressbar(main_frame, orient=tk.HORIZONTAL, mode='determinate')
        self.progress.grid(row=3, column=0, pady=10, sticky='ew')

        # Action buttons
        action_frame = ttk.Frame(main_frame)
        action_frame.grid(row=4, column=0, pady=15, sticky='ew')

        buttons = [
            ('🔄 Refresh List', self.update_file_list),
            ('📤 Process Selected', self.process_selected_files),
            ('⚙️ Process All Files', self.process_all_files),
            ('📦 Create Backup ZIP', self.create_backup_zip),
            ('🔒 Encrypt Files', self.encrypt_selected_files),
            ('🌐 Webhook Settings', self.show_webhook_info),
            ('📂 Create Folder', self.create_folder),
            ('📝 Create Test Files', self.create_test_files),
            ('👁️ Preview File', self.preview_file),
            #('📄 Create Dummy EDI Files', self.create_dummy_edi_files) # Removed from here
        ]
        for text, cmd in buttons:
            ttk.Button(action_frame, text=text, command=cmd).pack(side=tk.LEFT, padx=3)

        # Add the new button here
        ttk.Button(action_frame, text="📄 Create Dummy EDI Files", command=self.create_dummy_edi_files).pack(side=tk.LEFT, padx=3)


        # Status bar
        self.status_label = ttk.Label(
            main_frame,
            anchor=tk.CENTER,
            font=('Arial', 10),
            background='#f0f0f0'
        )
        self.status_label.grid(row=5, column=0, pady=10, sticky='ew')

    # --- Existing methods ---
    def browse_shared_folder(self):
        """Select shared folder"""
        folder = filedialog.askdirectory()
        if folder:
            self.current_shared_folder.set(os.path.join(folder, ""))
            self.update_file_list()

    def create_folders(self):
        """Create required directories"""
        for path in self.DEFAULT_PATHS.values():
            os.makedirs(path, exist_ok=True)
        self.log_message("All directories initialized")

    def scan_files(self):
        """Scan current shared folder"""
        folder = self.current_shared_folder.get()
        if not os.path.exists(folder):
            return []
        return [
            {
                "name": f,
                "path": os.path.join(folder, f),
                "extension": os.path.splitext(f)[1].lower()
            }
            for f in os.listdir(folder)
            if os.path.isfile(os.path.join(folder, f))
        ]

    def update_file_list(self):
        """Refresh file list display"""
        self.file_listbox.delete(0, tk.END)
        self.file_data = self.scan_files()
        for f in self.file_data:
            self.file_listbox.insert(tk.END, f"📄 {f['name']}")
        self.status_label.config(text=f"Loaded {len(self.file_data)} files from {self.current_shared_folder.get()}")
        self.log_message(f"File list updated - {len(self.file_data)} files found")

    def process_all_files(self):
        """Process all files through Zoho RPA workflow"""
        try:
            if not self.file_data:
                messagebox.showwarning("Warning", "No files to process!")
                return

            # Process files
            success_count = 0
            error_files = []

            for idx, file_info in enumerate(self.file_data):
                try:
                    # Update UI
                    self.update_progress(idx, len(self.file_data))
                    self.status_label.config(text=f"Processing {file_info['name']} ({idx+1}/{len(self.file_data)})")
                    self.window.update_idletasks()

                    # Process file
                    if self.process_single_file(file_info, idx, len(self.file_data)):
                        success_count += 1
                    else:
                        error_files.append(file_info["name"])
                except Exception as e:
                    self.log_error(f"Processing failed: {file_info['name']} - {str(e)}")
                    error_files.append(file_info["name"])

            # Show final report
            self.show_processing_report(success_count, error_files)
            self.update_file_list()
        except Exception as e:
            self.handle_error(e)

    def process_single_file(self, file_info, index, total):
        """Process individual file through webhook"""
        try:
            # Encrypt file
            encrypted_path = self.encrypt_file(file_info["path"])

            # Prepare payload
            payload = {
                "operation": "process_file",
                "file_name": file_info["name"],
                "current_index": index,
                "total_files": total,
                "timestamp": datetime.datetime.now().isoformat()
            }

            # Add headers
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.API_KEY}"
            }

            # Send to webhook
            response = requests.post(
                self.webhook_url.get(),
                headers=headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()

            # Verify response
            if response.json().get("status") == "success":
                self.log_message(f"Processed {file_info['name']} successfully")
                return True
            else:
                self.log_error(f"Webhook error: {response.text}")
                return False
        except requests.exceptions.RequestException as e:
            self.log_error(f"Webhook connection failed: {str(e)}")
            return False
        except Exception as e:
            self.log_error(f"Processing error: {str(e)}")
            return False

    def move_files(self, files, destination):
        """Move files to destination directory"""
        for f in files:
            try:
                dest_path = os.path.join(destination, f["name"])
                shutil.move(f["path"], dest_path)
                self.log_message(f"Moved {f['name']} to {destination}")
            except Exception as e:
                self.log_error(f"Failed to move {f['name']}: {str(e)}")

    def move_to_processed(self, file_info):
        """Move file to processed directory"""
        dest = os.path.join(self.DEFAULT_PATHS["processed"], file_info["name"])
        shutil.move(file_info["path"], dest)

    def move_to_error(self, file_info):
        """Move file to error directory"""
        dest = os.path.join(self.DEFAULT_PATHS["error"], file_info["name"])
        shutil.move(file_info["path"], dest)

    def create_backup(self, file_path):
        """Create encrypted backup of a file"""
        global file_name
        try:
            backup_dir = self.DEFAULT_PATHS["backup"]
            file_name = os.path.basename(file_path)
            backup_path = os.path.join(backup_dir, file_name)
            
            shutil.copy(file_path, backup_path)
            self.encrypt_file(backup_path)
            self.log_message(f"Created backup for {file_name}")
        except Exception as e:
            self.log_error(f"Backup failed for {file_name}: {str(e)}")

    def encrypt_file(self, file_path):
        """Encrypt file using Fernet encryption"""
        try:
            with open(file_path, 'rb') as f:
                data = f.read()
            encrypted_data = self.cipher_suite.encrypt(data)
            encrypted_path = f"{file_path}.enc"
            with open(encrypted_path, 'wb') as f:
                f.write(encrypted_data)
            os.remove(file_path)
            return encrypted_path
        except Exception as e:
            self.log_error(f"Encryption failed for {os.path.basename(file_path)}: {str(e)}")
            raise

    def test_webhook(self):
        """Test webhook connection"""
        try:
            test_payload = {
                "operation": "test_connection",
                "timestamp": datetime.datetime.now().isoformat(),
                "system": "File Manager Pro"
            }

            # Add headers
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.API_KEY}"
            }

            response = requests.post(
                self.webhook_url.get(),
                headers=headers,
                json=test_payload,
                timeout=10
            )
            response.raise_for_status()
            messagebox.showinfo("Webhook Test", f"✅ Connection successful!\nResponse code: {response.status_code}\n{response.text}")
        except Exception as e:
            messagebox.showerror("Webhook Test", f"❌ Connection failed: {str(e)}")

    def show_webhook_info(self):
        """Display webhook configuration info"""
        info = [
            "Current Webhook Configuration:",
            f"URL: {self.webhook_url.get()}",
            f"API Key: {self.API_KEY[:15]}...",
            f"Encryption Key: {self.encryption_key.decode()[:15]}..."
        ]
        messagebox.showinfo("Webhook Settings", "\n".join(info))

    def update_progress(self, current, total):
        """Update progress bar"""
        progress = (current + 1) / total * 100
        self.progress["value"] = progress
        self.window.update_idletasks()

    def show_processing_report(self, success, errors):
        """Show final processing report"""
        report = [
            "Processing Complete!",
            f"Successfully processed: {success} files",
            f"Failed files: {len(errors)}",
            f"Encryption key: {self.encryption_key.decode()}"
        ]
        if errors:
            report.append("\nError files:\n- " + "\n- ".join(errors))
        messagebox.showinfo("Processing Report", "\n".join(report))
        self.progress["value"] = 0
        self.status_label.config(text="Ready")

    def initialize_logging(self):
        """Initialize logging system"""
        try:
            log_dir = self.DEFAULT_PATHS["logs"]
            os.makedirs(log_dir, exist_ok=True)
            log_file = os.path.join(log_dir, f"activity_{datetime.datetime.now().strftime('%Y%m%d')}.log")
            
            self.log_file = open(log_file, 'a')
            self.log_file_initialized = True;
            self.log_message("Logging system initialized")
        except Exception as e:
            messagebox.showerror("Logging Error", f"Failed to initialize logs: {str(e)}")

    def log_message(self, message):
        """Write info message to log"""
        if self.log_file_initialized:
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.log_file.write(f"[{timestamp}] INFO: {message}\n")
            self.log_file.flush()

    def log_error(self, message):
        """Write error message to log"""
        if self.log_file_initialized:
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.log_file.write(f"[{timestamp}] ERROR: {message}\n")
            self.log_file.flush()

    def handle_error(self, error):
        """Handle critical errors"""
        self.log_error(str(error))
        messagebox.showerror("Critical Error", str(error))
        self.progress["value"] = 0
        self.status_label.config(text="Error occurred")

    def process_selected_files(self):
        """Process selected files only"""
        selected = self.file_listbox.curselection()
        if not selected:
            messagebox.showwarning("Warning", "No files selected!")
            return
            
        selected_files = [self.file_data[i] for i in selected]
        # Implement selected files processing logic here
        messagebox.showinfo("Info", f"Processing {len(selected_files)} selected files")

    def create_backup_zip(self):
        """Create encrypted ZIP backup"""
        try:
            zip_dir = self.DEFAULT_PATHS["zip"]
            os.makedirs(zip_dir, exist_ok=True)
            zip_name = os.path.join(zip_dir, f"backup_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.zip")
            with ZipFile(zip_name, 'w', ZIP_DEFLATED) as zipf:
                for root, _, files in os.walk(self.DEFAULT_PATHS["backup"]):
                    for file in files:
                        zipf.write(os.path.join(root, file))
            self.log_message(f"Created backup ZIP: {zip_name}")
            messagebox.showinfo("Success", f"Created backup archive: {os.path.basename(zip_name)}")
        except Exception as e:
            self.log_error(f"ZIP creation failed: {str(e)}")
            messagebox.showerror("Error", f"Failed to create ZIP: {str(e)}")

    def encrypt_selected_files(self):
        """Encrypt selected files"""
        selected = self.file_listbox.curselection()
        if not selected:
            messagebox.showwarning("Warning", "No files selected!")
            return
        for idx in selected:
            file_info = self.file_data[idx]
            try:
                self.encrypt_file(file_info["path"])
                self.log_message(f"Encrypted {file_info['name']}")
            except Exception as e:
                self.log_error(f"Failed to encrypt {file_info['name']}: {str(e)}")
        self.update_file_list()
        messagebox.showinfo("Encryption Complete", "Selected files encrypted successfully")

    def create_folder(self):
        """Create a new folder with a custom name"""
        folder_name = simpledialog.askstring("Create Folder", "Enter folder name:")
        if folder_name:
            folder_path = os.path.join(self.current_shared_folder.get(), folder_name)
            try:
                os.makedirs(folder_path, exist_ok=True)
                self.log_message(f"Created folder: {folder_path}")
                messagebox.showinfo("Success", f"Folder '{folder_name}' created successfully!")
                self.update_file_list()
            except Exception as e:
                self.log_error(f"Failed to create folder '{folder_name}': {str(e)}")
                messagebox.showerror("Error", f"Failed to create folder: {str(e)}")

    def create_test_files(self):
        """Create multiple test text files in the selected folder"""
        try:
            num_files = simpledialog.askinteger("Create Test Files", "Enter number of files to create:")
            if num_files and num_files > 0:
                for i in range(1, num_files + 1):
                    file_path = os.path.join(self.current_shared_folder.get(), f"test_file_{i}.txt")
                    with open(file_path, 'w') as f:
                        f.write(f"This is test file {i}.")
                self.log_message(f"Created {num_files} test files in {self.current_shared_folder.get()}")
                messagebox.showinfo("Success", f"Created {num_files} test files successfully!")
                self.update_file_list()
        except Exception as e:
            self.log_error(f"Failed to create test files: {str(e)}")
            messagebox.showerror("Error", f"Failed to create test files: {str(e)}")

    def create_dummy_edi_files(self):
        """Create multiple dummy EDI files in the selected folder"""
        try:
            num_files = simpledialog.askinteger("Create Dummy EDI Files", "Enter number of EDI files to create:")
            if num_files and num_files > 0:
                for i in range(1, num_files + 1):
                    file_path = os.path.join(self.current_shared_folder.get(), f"dummy_file_{i}.edi")
                    with open(file_path, 'w') as f:
                        f.write(f"ST*EDI*{i:04d}\nSE*1*{i:04d}\n")
                self.log_message(f"Created {num_files} dummy EDI files in {self.current_shared_folder.get()}")
                messagebox.showinfo("Success", f"Created {num_files} dummy EDI files successfully!")
                self.update_file_list()
        except Exception as e:
            self.log_error(f"Failed to create dummy EDI files: {str(e)}")
            messagebox.showerror("Error", f"Failed to create dummy EDI files: {str(e)}")

    def parse_edi_file(self, file_path):
        """Parse EDI file and extract key information"""
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            # Example: Extract transaction IDs (customize based on EDI format)
            transaction_ids = [line for line in content.splitlines() if line.startswith("ST")]
            messagebox.showinfo("EDI Parsing", f"Transaction IDs:\n" + "\n".join(transaction_ids))
        except Exception as e:
            self.log_error(f"Failed to parse EDI file: {str(e)}")
            messagebox.showerror("Error", f"Failed to parse EDI file: {str(e)}")

    def validate_files(self):
        """Validate EDI files for proper formatting"""
        invalid_files = []
        for file_info in self.file_data:
            if not file_info["name"].endswith(".edi"):
                invalid_files.append(file_info["name"])
        
        if invalid_files:
            messagebox.showwarning("Validation Warning", f"Invalid files detected:\n- " + "\n- ".join(invalid_files))
        else:
            messagebox.showinfo("Validation Success", "All files are valid!")

    def preview_file(self):
        """Preview the content of the selected file"""
        selected = self.file_listbox.curselection()
        if not selected:
            messagebox.showwarning("Warning", "No file selected!")
            return
        
        file_info = self.file_data[selected[0]]
        try:
            with open(file_info["path"], 'r') as f:
                content = f.read()
            preview_window = tk.Toplevel(self.window)
            preview_window.title(f"Preview - {file_info['name']}")
            text_widget = tk.Text(preview_window, wrap=tk.WORD)
            text_widget.insert(tk.END, content)
            text_widget.pack(fill=tk.BOTH, expand=True)
        except Exception as e:
            self.log_error(f"Failed to preview file {file_info['name']}: {str(e)}")
            messagebox.showerror("Error", f"Failed to preview file: {str(e)}")

    def handle_drag(self, event):
        """Handle drag event"""
        self.status_label.config(text="Drag detected")

    def handle_drop(self, event):
        """Handle drop event"""
        dropped_files = event.data.split()  # Assuming event.data contains file paths
        for file_path in dropped_files:
            if os.path.isfile(file_path):
                shutil.copy(file_path, self.current_shared_folder.get())
        self.status_label.config(text="Files added successfully")
        self.update_file_list()

if __name__ == "__main__":
    app = FileManagerApp()
    app.window.mainloop()