import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import sys
import logging
from pathlib import Path

# 嘗試匯入 reader，若失敗顯示錯誤
try:
    import reader
except ImportError:
    messagebox.showerror("錯誤", "找不到 reader.py，請確認 reader_gui.py 與 reader.py 在同一目錄下。")
    sys.exit(1)

class RedirectText:
    """將 stdout/stderr 重導向至 Tkinter Text 元件"""
    def __init__(self, text_widget):
        self.text_widget = text_widget

    def write(self, string):
        # 使用 after 確保在主執行緒更新 UI
        self.text_widget.after(0, lambda: self._append(string))

    def _append(self, string):
        try:
            self.text_widget.configure(state='normal')
            self.text_widget.insert(tk.END, string)
            self.text_widget.see(tk.END)
            self.text_widget.configure(state='disabled')
        except Exception:
            pass

    def flush(self):
        pass

class ReaderGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("TWSE 日線資料抓取器 (GUI)")
        self.root.geometry("700x550")
        
        # 設定樣式
        style = ttk.Style()
        try:
            style.theme_use('vista')  # Windows 原生風格嘗試
        except:
            pass
        style.configure("TButton", font=("Microsoft JhengHei", 10))
        style.configure("TLabel", font=("Microsoft JhengHei", 11))
        
        # 上方控制區
        control_frame = ttk.Frame(root, padding="15")
        control_frame.pack(fill=tk.X)
        
        ttk.Label(control_frame, text="抓取最近").pack(side=tk.LEFT)
        
        self.days_var = tk.StringVar(value="5")
        self.days_entry = ttk.Entry(control_frame, textvariable=self.days_var, width=5, justify="center")
        self.days_entry.pack(side=tk.LEFT, padx=5)
        
        ttk.Label(control_frame, text="個交易日").pack(side=tk.LEFT)
        
        self.start_btn = ttk.Button(control_frame, text="開始執行", command=self.on_start)
        self.start_btn.pack(side=tk.LEFT, padx=20)
        
        # 狀態顯示
        self.status_label = ttk.Label(control_frame, text="準備就緒", foreground="gray")
        self.status_label.pack(side=tk.LEFT, padx=5)

        # 輸出日誌區
        log_frame = ttk.LabelFrame(root, text=" 執行紀錄 ", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        self.log_area = scrolledtext.ScrolledText(log_frame, state='disabled', font=("Consolas", 9))
        self.log_area.pack(fill=tk.BOTH, expand=True)

        # 重導向 stdout / stderr
        self.redirector = RedirectText(self.log_area)
        sys.stdout = self.redirector
        sys.stderr = self.redirector

    def on_start(self):
        days_str = self.days_var.get()
        if not days_str.isdigit() or int(days_str) <= 0:
            messagebox.showwarning("輸入錯誤", "請輸入大於 0 的整數 days")
            return

        days = int(days_str)
        self.toggle_ui(running=True)
        self.status_label.config(text=f"正在執行 (最近 {days} 天)...", foreground="blue")
        
        # 清空 log
        self.log_area.configure(state='normal')
        self.log_area.delete(1.0, tk.END)
        self.log_area.configure(state='disabled')

        # 啟動執行緒
        threading.Thread(target=self.run_task, args=(days,), daemon=True).start()

    def run_task(self, days):
        try:
            # 建構假參數物件，模擬 argparse 的結果
            class Args:
                pass
            
            args = Args()
            args.days = days
            # 其他參數使用預設值
            args.date_from = None
            args.date_to = None
            args.sleep = 0.2
            args.max_retries = 3
            args.batch_size = 5000
            args.force = False
            args.log_level = "INFO"
            
            # 使用 reader.py 內定義的路徑 (確保一致性)
            args.data_dir = str(reader.DEFAULT_DATA_DIR)
            args.db_path = str(reader.DEFAULT_DB_PATH)
            
            args.out_format = "csv"
            args.from_cache_only = False
            args.refresh_calendar = False
            args.halt_on_fail = 20

            # 呼叫 reader 的主邏輯
            # reader 內部的 setup_logging 可能會重複添加 handler，但因為我們劫持了 stdout，所以沒關係
            reader.run(args)
            
            self.root.after(0, lambda: self.finish_task(success=True))
            
        except Exception as e:
            import traceback
            err_msg = traceback.format_exc()
            print(f"\n[System Error]\n{err_msg}")
            self.root.after(0, lambda: self.finish_task(success=False, error_msg=str(e)))

    def finish_task(self, success, error_msg=None):
        self.toggle_ui(running=False)
        if success:
            self.status_label.config(text="執行完成", foreground="green")
            messagebox.showinfo("完成", "資料抓取與入庫完成！\n詳細資訊請查看紀錄區。")
        else:
            self.status_label.config(text="執行失敗", foreground="red")
            messagebox.showerror("錯誤", f"執行過程發生錯誤：\n{error_msg}")

    def toggle_ui(self, running):
        state = 'disabled' if running else 'normal'
        self.start_btn.config(state=state)
        self.days_entry.config(state=state)

if __name__ == "__main__":
    root = tk.Tk()
    app = ReaderGUI(root)
    root.mainloop()
