"""
Application Launcher with Splash Screen

Provides a professional startup experience:
- Splash screen while loading
- System tray icon
- Auto-launch browser
- Graceful error handling
"""

import sys
import os
import threading
import webbrowser
import time
from pathlib import Path

# Add the application directory to path
if getattr(sys, 'frozen', False):
    # Running as compiled executable
    APP_DIR = Path(sys.executable).parent
else:
    # Running as script
    APP_DIR = Path(__file__).parent.parent

sys.path.insert(0, str(APP_DIR))

# Try to import tkinter for splash screen
try:
    import tkinter as tk
    from tkinter import ttk, messagebox
    HAS_TK = True
except ImportError:
    HAS_TK = False


class SplashScreen:
    """Professional splash screen with loading progress"""

    def __init__(self):
        self.root = tk.Tk()
        self.root.title("StatArb Pro")

        # Remove window decorations
        self.root.overrideredirect(True)

        # Window size
        width = 500
        height = 300

        # Center on screen
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        x = (screen_width - width) // 2
        y = (screen_height - height) // 2
        self.root.geometry(f'{width}x{height}+{x}+{y}')

        # Dark theme colors
        bg_color = '#1a1a2e'
        accent_color = '#e94560'
        text_color = '#ffffff'

        self.root.configure(bg=bg_color)

        # Main frame
        main_frame = tk.Frame(self.root, bg=bg_color)
        main_frame.pack(fill='both', expand=True, padx=2, pady=2)

        # Border effect
        main_frame.configure(highlightbackground=accent_color, highlightthickness=2)

        # Logo/Title area
        title_frame = tk.Frame(main_frame, bg=bg_color)
        title_frame.pack(fill='x', pady=(40, 20))

        # Application name
        title_label = tk.Label(
            title_frame,
            text="StatArb Pro",
            font=('Segoe UI', 32, 'bold'),
            fg=accent_color,
            bg=bg_color
        )
        title_label.pack()

        # Subtitle
        subtitle_label = tk.Label(
            title_frame,
            text="Multi-Broker Statistical Arbitrage System",
            font=('Segoe UI', 11),
            fg=text_color,
            bg=bg_color
        )
        subtitle_label.pack(pady=(5, 0))

        # Version
        version_label = tk.Label(
            title_frame,
            text="Version 1.0.0",
            font=('Segoe UI', 9),
            fg='#888888',
            bg=bg_color
        )
        version_label.pack(pady=(5, 0))

        # Status message
        self.status_var = tk.StringVar(value="Initializing...")
        status_label = tk.Label(
            main_frame,
            textvariable=self.status_var,
            font=('Segoe UI', 10),
            fg=text_color,
            bg=bg_color
        )
        status_label.pack(pady=(30, 10))

        # Progress bar
        style = ttk.Style()
        style.theme_use('clam')
        style.configure(
            "Custom.Horizontal.TProgressbar",
            troughcolor=bg_color,
            background=accent_color,
            darkcolor=accent_color,
            lightcolor=accent_color,
            bordercolor=bg_color
        )

        self.progress = ttk.Progressbar(
            main_frame,
            style="Custom.Horizontal.TProgressbar",
            length=400,
            mode='determinate'
        )
        self.progress.pack(pady=(0, 20))

        # Copyright
        copyright_label = tk.Label(
            main_frame,
            text="© 2024 StatArb Pro. All rights reserved.",
            font=('Segoe UI', 8),
            fg='#666666',
            bg=bg_color
        )
        copyright_label.pack(side='bottom', pady=(0, 15))

    def update_status(self, message: str, progress: int):
        """Update status message and progress bar"""
        self.status_var.set(message)
        self.progress['value'] = progress
        self.root.update()

    def close(self):
        """Close splash screen"""
        self.root.destroy()

    def mainloop(self):
        """Start the splash screen event loop"""
        self.root.mainloop()


class ApplicationLauncher:
    """Main application launcher"""

    def __init__(self):
        self.splash = None
        self.server_thread = None
        self.server_started = False
        self.startup_complete = False
        self.startup_error = None

    def show_error(self, title: str, message: str):
        """Show error dialog"""
        # Always print to console first
        print(f"\n{'='*50}")
        print(f"ERROR: {title}")
        print(f"{'='*50}")
        print(message)
        print(f"{'='*50}\n")

        # Try to show GUI dialog
        if HAS_TK:
            try:
                root = tk.Tk()
                root.withdraw()
                messagebox.showerror(title, message)
                root.destroy()
            except Exception as e:
                print(f"Could not show error dialog: {e}")

    def start_server(self):
        """Start the Flask server in background"""
        try:
            import traceback
            print("Importing web.app...")
            from web.app import init_app, socketio, app

            print("Initializing app...")
            init_app()
            self.server_started = True
            print("Starting server on http://127.0.0.1:5000")
            socketio.run(app, host='127.0.0.1', port=5000, debug=False, use_reloader=False)

        except Exception as e:
            import traceback
            self.server_started = False
            self.startup_error = f"{str(e)}\n\nTraceback:\n{traceback.format_exc()}"
            print(f"Server error: {e}")
            traceback.print_exc()

    def launch(self):
        """Launch the application"""

        # Show splash screen
        if HAS_TK:
            self.splash = SplashScreen()

            # Run startup in separate thread
            startup_thread = threading.Thread(target=self._startup_sequence)
            startup_thread.start()

            # Run splash screen main loop (blocks until splash closes)
            self.splash.mainloop()

            # After splash closes, open browser and keep running
            if self.startup_complete and not self.startup_error:
                webbrowser.open('http://127.0.0.1:5000')

                # Keep main thread alive while server runs
                if self.server_thread and self.server_thread.is_alive():
                    self.server_thread.join()
            elif self.startup_error:
                self.show_error("Startup Error", f"Failed to start application:\n\n{self.startup_error}")
                sys.exit(1)
        else:
            # No GUI, just start server
            self._startup_sequence_no_splash()

    def _startup_sequence_no_splash(self):
        """Run startup without splash screen"""
        print("Starting StatArb Pro...")
        self.server_thread = threading.Thread(target=self.start_server, daemon=True)
        self.server_thread.start()
        time.sleep(2)
        webbrowser.open('http://127.0.0.1:5000')
        if self.server_thread:
            self.server_thread.join()

    def _startup_sequence(self):
        """Run the startup sequence (called from background thread)"""
        try:
            steps = [
                ("Loading configuration...", 10),
                ("Initializing database...", 25),
                ("Loading trading engine...", 40),
                ("Starting web server...", 60),
                ("Preparing user interface...", 80),
                ("Almost ready...", 95),
            ]

            for message, progress in steps:
                if self.splash:
                    self.splash.update_status(message, progress)
                time.sleep(0.3)

            # Start server in background thread
            self.server_thread = threading.Thread(target=self.start_server, daemon=True)
            self.server_thread.start()

            # Wait for server to start
            time.sleep(2)

            if self.splash:
                self.splash.update_status("Launching browser...", 100)
                time.sleep(0.5)

            # Mark startup complete before closing splash
            self.startup_complete = True

            # Close splash - this will cause mainloop() to return in main thread
            if self.splash:
                self.splash.close()

        except Exception as e:
            self.startup_error = str(e)
            if self.splash:
                self.splash.close()


def main():
    """Main entry point"""
    launcher = ApplicationLauncher()
    launcher.launch()


if __name__ == '__main__':
    main()
