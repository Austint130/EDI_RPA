#!/usr/bin/env python3
import logging
import os
import shutil
import sys
import time
import datetime
import subprocess
import json
import traceback
import ftplib # For FTP uploads
import argparse # For command-line arguments
import signal # For graceful shutdown

# ==========================================================
# Configuration Defaults and Handling
# ==========================================================
CONFIG_FILE = 'config.json'
LOG_FILE = 'edi_processor_background.log' # Log file for this version
# STATE_FILE = "schedule_state.json" # State file less relevant for background script

# Define defaults (used if config missing or keys missing)
DEFAULT_NOTIFICATION_SETTINGS = {"recipient_email": "", "send_on_error": True, "send_on_success_summary": False, "include_original_filename": True, "include_new_filename": True, "include_timestamp": True, "include_error_details": True, "include_success_fail_counts": True, "subject_error": "EDI Process Error", "subject_summary": "EDI Process Batch Summary"}
DEFAULT_CLIENT_PROFILE = {"source_folder": "", "processing_folder": "", "processed_folder": "", "secured_folder": "", "error_folder": "", "ftp_host": "", "ftp_port": 21, "ftp_user": "", "ftp_password": "", "ftp_remote_path": "/"}
DEFAULT_CONFIG = {"source_folder": "", "processing_folder": "", "processing_folder": "", "processed_folder": "", "secured_folder": "", "error_folder": "", "filezilla_exe_path": "", "ignore_keywords": ["log", "temp", "tmp"], "edi_extension": ".edi", "schedule_interval_minutes": 5, "auto_restart_schedule": False, "special_instructions": [], "notification_settings": DEFAULT_NOTIFICATION_SETTINGS.copy(), "active_client": None, "clients": {}}

# --- Standard Python Logging Setup ---
def setup_logging():
    """Configures logging to console and file."""
    log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger()
    logger.setLevel(logging.INFO) # Set minimum level to log

    # File Handler
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(log_formatter)
    logger.addHandler(file_handler)

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)

    return logger

logger = setup_logging() # Initialize logger

# --- Config Load/Save Functions (Modified for background logging) ---
def load_configuration():
    config = DEFAULT_CONFIG.copy()
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f: loaded_config = json.load(f)
            config.update(loaded_config)
            config.setdefault('notification_settings', {}).update({k:v for k,v in DEFAULT_NOTIFICATION_SETTINGS.items() if k not in config.get('notification_settings',{})})
            config.setdefault('clients', {}); config.setdefault('active_client', None); config.setdefault('special_instructions', [])
            default_instr_keys = {"enabled": False, "contains": "", "new_filename": "", "open_exe": "", "send_email": False, "email_subject": "", "email_body": ""}
            validated_instr = [{**default_instr_keys, **instr} for instr in config.get('special_instructions', []) if isinstance(instr, dict)]
            config['special_instructions'] = validated_instr
            logger.info("Config loaded successfully.")
        except Exception as e: logger.error(f"Failed load config: {e} - Using defaults.")
    else: logger.warning(f"{CONFIG_FILE} not found. Using/creating defaults."); save_configuration(config)
    return config

def save_configuration(config_data): # Less critical for bg script unless state changes
    config_data.setdefault('clients', {}); config_data.setdefault('active_client', None); config_data.setdefault('notification_settings', {}); config_data.setdefault('special_instructions', [])
    try:
        with open(CONFIG_FILE, 'w') as f: json.dump(config_data, f, indent=4)
        logger.info(f"Config saved to {CONFIG_FILE}"); return True
    except Exception as e: logger.error(f"Failed save config: {e}"); return False
# ----------------------------------------------------------

def parse_file(file_path):
    try:
        with open(file_path, 'r') as f: contents = f.read()
        if "fail" in contents.lower(): raise ValueError("'fail' keyword")
        time.sleep(0.1); return True # Simulate work
    except Exception as e: raise ValueError(f"Parsing failed: {e}") from e

# --- Get Effective Settings (Copied from GUI version) ---
def get_effective_settings(base_config, requested_client_name=None):
     """Merges general config with active client overrides."""
     # Use requested client if provided, else fallback to config's active_client
     active_name = requested_client_name if requested_client_name else base_config.get('active_client')
     clients = base_config.get('clients', {})
     effective = {k: v for k, v in base_config.items() if k not in ['clients', 'active_client']} # Copy base minus client info

     if active_name and active_name in clients:
          profile = clients[active_name]; effective["_client_name"] = active_name # Mark active client name
          logger.info(f"Using settings for active client: {active_name}")
          # Override general settings with non-empty client settings
          for key, value in profile.items():
               if value or isinstance(value, (int, float, bool)):
                   if value or key == 'ftp_password': effective[key] = value
     elif active_name:
         logger.warning(f"Requested active client '{active_name}' not found in configuration profiles.")
     else:
         logger.warning("No active client specified or found. Using general default settings.")

     # Final check for core folders
     for key in ['source_folder', 'processing_folder', 'processed_folder', 'secured_folder', 'error_folder']:
          effective.setdefault(key, DEFAULT_CONFIG.get(key))

     return effective

# --- Email Simulation (Copied from GUI version) ---
def send_notification_email(subject, body, config):
    recipient = config.get('notification_settings', {}).get('recipient_email');
    if not recipient: logger.warning("Email recipient missing in config, cannot send."); return
    log_output = (f"\n--- Email Simulation ---\n"
                  f"To: {recipient}\n"
                  f"Subject: {subject}\n"
                  f"----------------------\n"
                  f"{body}\n"
                  f"----------------------")
    logger.info(f"Simulated sending email '{subject}' to {recipient}")
    print(log_output) # Also print simulation to console easily
    # Add real smtplib logic here if needed for production background script

def build_email_body(event_type, details, config_override):
    cfg = config_override; notif = cfg.get('notification_settings', {}); body = ["EDI Notification", "="*30]
    if notif.get('include_timestamp', True): body.append(f"Time: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}")
    active_client = cfg.get('_client_name', cfg.get('active_client')); if active_client: body.append(f"Client: {active_client}")
    if event_type == 'error': body.extend(["\n--- ERROR ---", f"Ctx: {details.get('context', 'N/A')}", f"Op: {details.get('op', 'N/A')}"]);
    if notif.get('include_original_filename', True): body.append(f"Orig: {details.get('orig', 'N/A')}")
    if notif.get('include_new_filename', True) and details.get('new'): body.append(f"New: {details['new']}")
    if notif.get('include_error_details', True): body.append(f"\nMsg:\n{details.get('err', 'N/A')}")
    if details.get('move_err'): body.append(f"\nMoveErr: {details['move_err']}")
    elif event_type == 'summary': body.append("\n--- SUMMARY ---");
    if notif.get('include_success_fail_counts', True): body.extend([f"OK: {details.get('success_count', 'N/A')}", f"Fail: {details.get('fail_count', 'N/A')}", f"Skip: {details.get('skipped_count', 'N/A')}"])
    else: body.extend([f"\nEvt: {event_type}", f"Details: {details}"])
    body.append("\n" + "="*30); return "\n".join(body)

# --- Action Functions (Adapted from GUI version) ---
def launch_exe(exe_path, config):
     """Checks if EXE is running (Windows only) and launches if not."""
     if not (exe_path and os.path.isfile(exe_path)): logger.warning(f"EXE invalid/missing: '{exe_path}'"); return
     exe_name = os.path.basename(exe_path)
     try: # Check tasklist (Windows specific)
          startupinfo = subprocess.STARTUPINFO(); startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW; startupinfo.wShowWindow = subprocess.SW_HIDE
          output = subprocess.check_output(['tasklist', '/FI', f'IMAGENAME eq {exe_name}'], text=True, startupinfo=startupinfo, stderr=subprocess.STDOUT)
          if any(line.lower().startswith(exe_name.lower()) for line in output.strip().split('\n')):
               logger.info(f"EXE '{exe_name}' already running. Skipping.")
               return
     except FileNotFoundError: logger.warning("tasklist not found. Cannot check if EXE running.")
     except subprocess.CalledProcessError: pass # Process not found, continue to launch
     except Exception as check_e: logger.warning(f"Error checking tasklist for {exe_name}: {check_e}")
     # Launch
     try:
          logger.info(f"Launch EXE: {exe_path}"); p=subprocess.Popen([exe_path], shell=False); logger.info(f"Launched PID: {p.pid}")
     except Exception as e:
         logger.error(f"EXE Launch Error '{exe_path}': {e}")
         notif_cfg = config.get('notification_settings',{})
         if notif_cfg.get('send_on_error'): d={"op":"LaunchEXE","exe":exe_path,"err":str(e)}; s=notif_cfg.get('subject_error'); b=build_email_body('error', d, config); send_notification_email(s,b,config)

def upload_file_ftp(local_file_path, ftp_details):
    """Uploads a file via FTP using details dict. Returns True/False."""
    host=ftp_details.get('ftp_host'); port=ftp_details.get('ftp_port',21); user=ftp_details.get('ftp_user'); password=ftp_details.get('ftp_password'); remote_path=ftp_details.get('ftp_remote_path','/')
    if not all([host, user, password is not None]): logger.warning(f"FTP Skip: Missing creds for {os.path.basename(local_file_path)}."); return False
    if not remote_path.endswith('/'): remote_path += '/'
    remote_filename=os.path.basename(local_file_path); full_remote_path=remote_path+remote_filename; ftp=None
    try:
        logger.info(f"FTP Connect: {host}:{port}..."); ftp=ftplib.FTP(); ftp.connect(host,port,timeout=30); ftp.login(user,password); logger.info("FTP OK.")
        logger.info(f"FTP Upload: '{os.path.basename(local_file_path)}' -> '{full_remote_path}'...")
        try: ftp.mkd(remote_path); logger.info(f"FTP created remote dir: {remote_path}")
        except ftplib.error_perm as e:
             if '550' in str(e): pass # Ignore "already exists"
             else: raise # Reraise other errors
        with open(local_file_path,'rb') as f: ftp.storbinary(f'STOR {full_remote_path}',f)
        logger.info(f"FTP OK: {remote_filename}."); return True
    except ftplib.all_errors as e: logger.error(f"FTP Error ({remote_filename}): {e}"); return False
    except Exception as e: logger.error(f"FTP Unexpected Error ({remote_filename}): {e}"); return False
    finally:
         if ftp: try: ftp.quit()
         except: pass

# --- Core Processing Logic (Adapted from GUI version) ---
def process_edi_files(effective_config):
    """ Core processing logic. Uses effective config for the active client."""
    cfg = effective_config # Use the passed-in effective config
    src=cfg.get('source_folder'); proc=cfg.get('processing_folder'); pced=cfg.get('processed_folder'); sec=cfg.get('secured_folder'); err_fld=cfg.get('error_folder')
    ignore_kw=cfg.get('ignore_keywords',[]); edi_ext=cfg.get('edi_extension','.edi'); instructions=cfg.get('special_instructions',[]); notif=cfg.get('notification_settings',{})
    client_ftp_details=cfg

    if not src: logger.error("Source folder missing in effective config!"); return 0,0,0
    if not all([proc, pced, sec, err_fld]): logger.error("Core folders missing!"); return 0,0,0
    try: # Ensure source dir exists
         if not os.path.isdir(src): logger.warning(f"Source '{src}' missing, creating."); os.makedirs(src,exist_ok=True); logger.info(f"Source '{src}' created.")
    except Exception as mkdir_e: logger.error(f"CRITICAL: Cannot create source '{src}': {mkdir_e}"); return 0,0,0
    for f in [proc,pced,sec,err_fld]: os.makedirs(f, exist_ok=True) # Ensure other dirs

    s_c, f_c, k_c = 0,0,0; to_process = []
    logger.info(f"Checking source: {src}")
    try: # Collect
        found_files=os.listdir(src); logger.info(f"Found {len(found_files)} items: {found_files}")
        for fn in found_files:
             fp=os.path.join(src,fn); logger.debug(f"Chk: {fn}|File? {os.path.isfile(fp)}")
             if not os.path.isfile(fp): k_c+=1; continue
             fnl=fn.lower(); ignore=False;
             for kw in (ignore_kw or []):
                  if kw and kw.lower() in fnl: logger.info(f"Ignore '{fn}' kw '{kw}'."); ignore=True; k_c+=1; break
             if ignore: continue
             edi_ext_lower = (edi_ext or '.edi').lower()
             if not fnl.endswith(edi_ext_lower): logger.info(f"Ignore '{fn}' ext (expect '{edi_ext_lower}')."); k_c+=1; continue
             logger.info(f"Adding '{fn}' to list.")
             to_process.append(fn)
    except FileNotFoundError: logger.error(f"Source folder vanished: {src}"); return s_c,f_c,k_c
    except Exception as e: logger.error(f"List Src Error ({src}): {e}"); return s_c,f_c,k_c+len(to_process)
    if not to_process: logger.info(f"No valid EDI files in {src}."); return s_c,f_c,k_c

    moved=[] # Move
    logger.info(f"Moving {len(to_process)} files to {proc}...")
    for i, fn in enumerate(to_process):
        try: shutil.move(os.path.join(src, fn), os.path.join(proc, fn)); moved.append(fn)
        except Exception as e: logger.error(f"Move Err '{fn}': {e}"); f_c+=1; if notif.get('send_on_error'): d={"op":"Move","orig":fn,"err":str(e)}; s=notif.get('subject_error'); b=build_email_body('error',d,cfg); send_notification_email(s,b,cfg) # Direct call okay here
    time.sleep(0.1)

    f_ctr = 1 # Process
    logger.info(f"Processing {len(moved)} files from {proc}...")
    for i, fn in enumerate(moved):
        orig_p=os.path.join(proc,fn); n_fn=f"p_{datetime.datetime.now():%y%m%d%H%M%S}_{f_ctr}{edi_ext}"; n_p=None; op="Start"; instr_m={}
        logger.info(f"Processing file {i+1}/{len(moved)}: {fn}")
        try:
            op="CheckInstr"
            for instr in instructions:
                if instr.get("enabled") and instr.get("contains") and instr["contains"].lower() in fn.lower(): base=instr.get('new_filename',n_fn).rsplit('.',1)[0]; n_fn=f"{base}_{datetime.datetime.now():%y%m%d%H%M%S}_{i+1}{edi_ext}"; instr_m=instr; logger.info(f"Match rule '{instr['contains']}'."); break
            else: f_ctr+=1
            n_p=os.path.join(proc,n_fn); op="Rename";
            if not os.path.exists(orig_p): raise FileNotFoundError("Gone")
            os.rename(orig_p,n_p); logger.info(f"Renamed '{fn}'->'{n_fn}'")
            op="Parse"; parse_file(n_p); logger.info(f"Parsed '{n_fn}'")
            op="MovePced"; pced_d=os.path.join(pced,n_fn); shutil.move(n_p,pced_d); logger.info(f"Moved '{n_fn}' to Pced")
            pced_fp=pced_d; sec_fp=os.path.join(sec,n_fn)
            # Post-processing
            if instr_m.get('open_exe'): launch_exe(instr_m['open_exe'], cfg) # Pass effective config
            if instr_m.get('send_email'): s=instr_m.get('email_subject'); bt=instr_m.get('email_body'); b=bt.replace("{orig}",fn).replace("{new}",n_fn); send_notification_email(s,b,cfg) # Pass effective config
            # FTP Upload
            if client_ftp_details.get('ftp_host') and client_ftp_details.get('ftp_user'):
                 op = "FTP Upload"; logger.info(f"Attempting FTP for '{n_fn}'...")
                 upload_ok = upload_file_ftp(pced_fp, client_ftp_details)
                 if upload_ok: logger.info(f"FTP OK: '{n_fn}'.")
                 else: logger.error(f"FTP FAIL: '{n_fn}'."); if notif.get('send_on_error'): d={"op":op,"orig":fn,"new":n_fn,"err":"FTP Failed"}; s=notif.get('subject_error'); b=build_email_body('error',d,cfg); send_notification_email(s,b,cfg)
            op="MoveSec"; shutil.move(pced_fp, sec_fp); logger.info(f"Secured '{n_fn}'"); s_c+=1
        except Exception as e:
            f_c+=1; emsg=f"ERROR '{fn}' @ '{op}': {e}"; logger.error(emsg); logger.debug(traceback.format_exc()) # Debug level for full trace
            err_d={"op":op,"orig":fn,"new":n_fn if instr_m else None,"err":str(e),"trace":traceback.format_exc()}
            err_n=n_fn if instr_m else fn; err_dp=os.path.join(err_fld, err_n)
            f_move=n_p if os.path.exists(n_p or '') else orig_p
            if os.path.exists(f_move):
                try: shutil.move(f_move, err_dp); logger.info(f"Moved err file '{os.path.basename(f_move)}'")
                except Exception as me: logger.error(f"FAIL Move Err: {me}"); err_d["move_err"]=str(me)
            else: logger.warning(f"Cannot find err file '{f_move}'")
            if notif.get('send_on_error'): s=notif.get('subject_error'); b=build_email_body('error',err_d,cfg); send_notification_email(s,b,cfg)
    logger.info("Batch finished.")
    return s_c, f_c, k_c

# --- Global flag for loop control ---
keep_running = True

def signal_handler(sig, frame):
    """Handles Ctrl+C or termination signals for graceful shutdown."""
    global keep_running
    logger.warning(f"Received signal {sig}. Shutting down loop...")
    keep_running = False

# ==========================================================
# Main Execution Block
# ==========================================================
if __name__ == "__main__":
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description="EDI Processor (Background Script)")
    parser.add_argument("-c", "--client", required=True, help="Name of the client profile to use (must exist in config.json)")
    parser.add_argument("-m", "--mode", choices=['single', 'loop'], default='loop', help="Run mode: 'single' for one check, 'loop' for continuous checking (default: loop)")
    parser.add_argument("-i", "--interval", type=int, help="Override check interval in minutes (only used in loop mode)")
    args = parser.parse_args()

    logger.info("--- EDI Processor Background Script Starting ---")
    logger.info(f"Mode: {args.mode}, Client: {args.client}")

    # --- Load base configuration ---
    base_config = load_configuration()

    # --- Get settings for the specified client ---
    effective_config = get_effective_settings(base_config, requested_client_name=args.client)

    # --- Validate essential client settings ---
    if not effective_config.get("_client_name"): # Check if client profile was actually found
         logger.error(f"Client profile '{args.client}' not found in {CONFIG_FILE}. Exiting.")
         sys.exit(1)
    if not effective_config.get("source_folder"):
         logger.error(f"Source folder not configured for client '{args.client}'. Exiting.")
         sys.exit(1)

    # --- Determine run interval ---
    # Use command-line override if provided, else use effective config, else default
    run_interval_minutes = args.interval if args.interval is not None else effective_config.get('schedule_interval_minutes', 5)
    if not isinstance(run_interval_minutes, int) or run_interval_minutes <= 0:
        logger.warning(f"Invalid interval ({run_interval_minutes}), using 5 minutes.")
        run_interval_minutes = 5

    # --- Set up signal handling for graceful exit in loop mode ---
    signal.signal(signal.SIGINT, signal_handler)  # Handle Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler) # Handle termination signal

    # --- Main Execution Logic ---
    try:
        if args.mode == 'single':
            logger.info("Running in single mode...")
            s, f, k = process_edi_files(effective_config)
            logger.info(f"Single run complete. Success={s}, Failed={f}, Skipped={k}")
            # Send summary email if configured
            notif_cfg = effective_config.get('notification_settings', {})
            if notif_cfg.get('send_on_success_summary'):
                 details={"success_count":s,"fail_count":f,"skipped_count":k}; subj=notif_cfg.get('subject_summary'); body=build_email_body('summary',details,effective_config); send_notification_email(subj,body,effective_config)

        elif args.mode == 'loop':
            logger.info(f"Running in loop mode. Interval: {run_interval_minutes} min. Press Ctrl+C to stop.")
            while keep_running:
                logger.info("Starting scheduled check cycle...")
                s, f, k = process_edi_files(effective_config)
                logger.info(f"Check cycle complete. Success={s}, Failed={f}, Skipped={k}")
                # Send summary email if configured
                notif_cfg = effective_config.get('notification_settings', {})
                if notif_cfg.get('send_on_success_summary'):
                    details={"success_count":s,"fail_count":f,"skipped_count":k}; subj=notif_cfg.get('subject_summary'); body=build_email_body('summary',details,effective_config); send_notification_email(subj,body,effective_config)

                # Wait for the next interval, checking keep_running flag
                logger.info(f"Waiting for {run_interval_minutes} minute(s)...")
                wait_seconds = run_interval_minutes * 60
                for _ in range(wait_seconds):
                    if not keep_running: break
                    time.sleep(1) # Sleep 1 second at a time
                if not keep_running: break # Exit outer loop if flag changed during sleep

    except Exception as main_e:
        logger.error(f"An uncaught error occurred: {main_e}", exc_info=True)
        # Attempt to send error email
        notif_cfg = effective_config.get('notification_settings', {})
        if notif_cfg.get('send_on_error'):
             details={"error":str(main_e),"context":"Main execution","trace":traceback.format_exc()}; subj=notif_cfg.get('subject_error'); body=build_email_body('error',details,effective_config); send_notification_email(subj,body,effective_config)

    logger.info("--- EDI Processor Background Script Finished ---")