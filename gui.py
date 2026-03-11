import tkinter as tk
from tkinter import ttk, messagebox
import threading
import subprocess
import sqlite3
from pathlib import Path

DB_PATH = "data/DB/highscore.db"

class SkycardsGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Skycards DB Manager")
        self.root.geometry("500x400")
        self.root.configure(bg="#2b2b2b")
        
        # Style
        style = ttk.Style()
        style.theme_use('clam')
        style.configure("TButton", font=("Helvetica", 11), padding=10)
        style.configure("Title.TLabel", font=("Helvetica", 18, "bold"), foreground="white", background="#2b2b2b")
        style.configure("TLabel", background="#2b2b2b", foreground="white", font=("Helvetica", 10))
        
        # Main frame
        main_frame = ttk.Frame(root, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Title
        title = ttk.Label(main_frame, text="Skycards Database Manager", style="Title.TLabel")
        title.pack(pady=20)
        
        # Database stats
        stats_frame = ttk.LabelFrame(main_frame, text="Database Stats", padding="10")
        stats_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        self.stats_label = ttk.Label(stats_frame, text="Loading...", justify=tk.LEFT)
        self.stats_label.pack(anchor="w", pady=10)
        
        self.load_stats()
        
        # Button frame
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.BOTH, expand=True, pady=20)
        
        # Refresh button
        refresh_btn = ttk.Button(button_frame, text="🔄 Refresh All Data", command=self.refresh_data)
        refresh_btn.pack(fill=tk.X, pady=5)
        
        # Quick refresh button
        quick_refresh_btn = ttk.Button(button_frame, text="⚡ Quick Refresh (4 threads)", command=self.quick_refresh)
        quick_refresh_btn.pack(fill=tk.X, pady=5)
        
        # View database button
        view_btn = ttk.Button(button_frame, text="📊 View Database", command=self.view_database)
        view_btn.pack(fill=tk.X, pady=5)
        
        # Exit button
        exit_btn = ttk.Button(button_frame, text="❌ Exit", command=self.root.quit)
        exit_btn.pack(fill=tk.X, pady=5)
        
        # Progress frame
        progress_frame = ttk.Frame(main_frame)
        progress_frame.pack(fill=tk.X, pady=10)
        
        self.progress = ttk.Progressbar(progress_frame, mode='indeterminate')
        self.progress.pack(fill=tk.X)
        
        self.status_label = ttk.Label(main_frame, text="", foreground="#00d4ff")
        self.status_label.pack(pady=5)
    
    def load_stats(self):
        """Load and display database statistics"""
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM airport_highscore")
            total_players = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM airport_highscore WHERE userName != ''")
            players_with_names = cursor.fetchone()[0]
            
            cursor.execute("SELECT MAX(userXP) FROM airport_highscore")
            max_xp = cursor.fetchone()[0] or 0
            
            conn.close()
            
            stats_text = f"Total Players: {total_players}\nPlayers with Names: {players_with_names}\nMax XP: {max_xp:,}"
            self.stats_label.config(text=stats_text)
        except Exception as e:
            self.stats_label.config(text=f"Error loading stats: {str(e)}")
    
    def refresh_data(self):
        """Run refresh with 64 threads"""
        if messagebox.askyesno("Confirm", "Start full refresh with 64 threads?\nThis may take a while."):
            self.run_refresh(64)
    
    def quick_refresh(self):
        """Run quick refresh with 4 threads"""
        if messagebox.askyesno("Confirm", "Start quick refresh with 4 threads?"):
            self.run_refresh(4)
    
    def run_refresh(self, threads):
        """Run refresh in background thread"""
        def refresh_worker():
            try:
                self.progress.start()
                self.status_label.config(text=f"Refreshing with {threads} threads...")
                self.root.update()
                
                result = subprocess.run(
                    ["python3", "Refresh.py", "--threads", str(threads)],
                    capture_output=True,
                    text=True,
                    timeout=3600
                )
                
                self.progress.stop()
                
                if result.returncode == 0:
                    self.status_label.config(text="✓ Refresh complete!", foreground="#00ff00")
                    messagebox.showinfo("Success", "Database refresh completed successfully!")
                    self.load_stats()
                else:
                    self.status_label.config(text="✗ Refresh failed", foreground="#ff0000")
                    messagebox.showerror("Error", f"Refresh failed:\n{result.stderr}")
            except Exception as e:
                self.progress.stop()
                self.status_label.config(text="✗ Error", foreground="#ff0000")
                messagebox.showerror("Error", f"Failed to run refresh:\n{str(e)}")
        
        thread = threading.Thread(target=refresh_worker, daemon=True)
        thread.start()
    
    def view_database(self):
        """Open database viewer in default SQLite viewer"""
        try:
            db_path = Path(DB_PATH).resolve()
            subprocess.Popen(["open", str(db_path)])
            messagebox.showinfo("Info", f"Opening database:\n{db_path}")
        except Exception as e:
            messagebox.showerror("Error", f"Could not open database:\n{str(e)}")


if __name__ == "__main__":
    root = tk.Tk()
    app = SkycardsGUI(root)
    root.mainloop()
