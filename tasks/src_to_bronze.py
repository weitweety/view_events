from src.stream_loader import ingest_stream

ingest_stream(
    input_path="/dbfs/Volumes/dev/default/events_stream",
    table_name="dev.default.view_events",
    checkpoint_path="dbfs:/Volumes/dev/default/autoloader_checkpoints/view_events"
)