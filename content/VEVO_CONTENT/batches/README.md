# VEVO Batches

Store VEVO batch candidate lists, batch summaries, and per-batch notes here.

Recommended candidate filename:

```text
batch-YYYY-MM-DD-topic-candidates.txt
```

Before import, run:

```powershell
python -X utf8 content\VEVO_CONTENT\tools\vevo_duplicate_guard.py --file content\VEVO_CONTENT\batches\<file>.txt
```
