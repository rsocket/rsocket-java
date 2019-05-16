1. Start socat. It is used for emulation of transport disconnects

`socat -d TCP-LISTEN:8001,fork,reuseaddr TCP:localhost:8000`

2. start `ResumeFileTransfer.main`

3. terminate/start socat periodically for session resumption

`ResumeFileTransfer` output is as follows

```
Received file chunk: 7. Total size: 112
Received file chunk: 8. Total size: 128
Received file chunk: 9. Total size: 144
Received file chunk: 10. Total size: 160
Disconnected. Trying to resume connection...
Disconnected. Trying to resume connection...
Disconnected. Trying to resume connection...
Disconnected. Trying to resume connection...
Disconnected. Trying to resume connection...
Received file chunk: 11. Total size: 176
Received file chunk: 12. Total size: 192
Received file chunk: 13. Total size: 208
Received file chunk: 14. Total size: 224
Received file chunk: 15. Total size: 240
Received file chunk: 16. Total size: 256
```

It transfers file from `resources/lorem.txt` to `build/out/lorem_output.txt` in chunks of 16 bytes every 500 millis
