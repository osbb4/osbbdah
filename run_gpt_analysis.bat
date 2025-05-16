@echo off
echo 🔄 Environment activation
call venv\Scripts\activate.bat

echo 🧠 GPT-analysis of messages started...
python analyze_gpt.py

echo ✅ Done.
pause
