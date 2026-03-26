# ১. পাইথন ইমেজ ব্যবহার করা (Python 3.10)
FROM python:3.10-slim

# ২. কাজের ডিরেক্টরি সেট করা
WORKDIR /app

# ৩. সিস্টেমে FFmpeg এবং অন্যান্য প্রয়োজনীয় টুলস ইন্সটল করা (মাস্ট)
RUN apt-get update && apt-get install -y \
    ffmpeg \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# ৪. লাইব্রেরি ফাইলগুলো কপি করা
COPY requirements.txt .

# ৫. সব লাইব্রেরি ইন্সটল করা
RUN pip install --no-cache-dir -r requirements.txt

# ৬. আপনার সব ফাইল কপি করা
COPY . .

# ৭. বোট চালু করার কমান্ড
CMD ["python", "app.py"]
