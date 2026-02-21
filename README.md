# NSCOM01-MCO1

## Project Overview

This project implements a reliable data transfer over UDP in Java which provides TCP-inspired features including connection establishment, reliable delivery, ordered packet delivery, and session management. The implementation demonstrates how application-layer protocols can achieve reliability on top of UDP's best-effort delivery.

## Members
- Sky Hannah Parado (S12)
- Rhian Claire Yamsuan (S12)


## Program Structure
```
NSCOM01-MCO1/
├── Message.java      # Protocol message definition
├── Client.java       # Client application
├── Server.java       # Server application
├── Colors.java       # ANSI terminal color utilities
└── README.md         # This file
```

---

## Prerequisites

- Java JDK 8 or higher
- Two terminal windows (one for the server, one for the client)

---

## How to Run

### Step 1 — Compile

Open a terminal in the project directory and compile all files together:

```bash
javac Colors.java Message.java Server.java Client.java
```

All four files must be compiled at the same time since `Server` and `Client` both depend on `Message` and `Colors`.

---

### Step 2 — Start the Server

In **Terminal 1**, run:

```bash
java Server
```

You will be prompted:

```
Enter server port: 9000
```

Enter any available port (e.g. `9000`). The server will start listening and print:

```
Server listening on port: 9000
===== SERVER STARTED =====
[SERVER] Waiting for client...
```

> **Important:** Any files you want clients to download must be placed in the **same directory where you run the server**.

---

### Step 3 — Start the Client

In **Terminal 2**, run:

```bash
java Client
```

You will be prompted for the following:

```
Enter client port: 8000
Enter server host (localhost): localhost
Enter server port: 9000
```

- **Client port** — any available port different from the server's (e.g. `8000`)
- **Server host** — enter localhost to default to `localhost`, or type an IP address
- **Server port** — must match the port the server is listening on (e.g. `9000`)

A successful connection will print:

```
===== CONNECTION ESTABLISHED =====
[CLIENT] Session parameters:
         Server      : localhost:9000
         ISN         : 1
         Max payload : 1000 bytes
         Timeout     : 5000 ms
         Max retries : 3
```

---

### Step 4 — Transfer Files

After connecting, the client menu will appear:

```
===== File Transfer Functionality =====
[1] Download File
[2] Upload File
[X] Disconnect
Choose option:
```

#### Download a File

1. Choose `1`
2. Enter the **remote filename** — the name of a file that exists in the server's directory (e.g. `document.pdf`)
3. Enter the **local filename** — what to save it as on your machine (e.g. `received.pdf`)

```
Enter remote filename (on server): document.pdf
Enter local filename to save as: received.pdf
```

#### Upload a File

1. Choose `2`
2. Enter the **local filename** — a file that exists in your current directory (e.g. `students.csv`)
3. Enter the **remote filename** — what to save it as on the server (e.g. `uploaded.csv`)

```
Enter local filename to upload: students.csv
Enter remote filename to save as: uploaded.csv
```

#### Disconnect

Choose `X` (or `x`) to cleanly terminate the session. This triggers a FIN/FIN-ACK exchange.

---

## Verifying a Transfer

After a download or upload, verify the file was transferred correctly using the `fc` command (Windows) or `diff` (Linux/Mac):

**Windows:**
```bash
fc /b document.pdf received.pdf
```

**Linux / Mac:**
```bash
diff document.pdf received.pdf
```

If no differences are reported, the file was transferred byte-for-byte correctly.

---