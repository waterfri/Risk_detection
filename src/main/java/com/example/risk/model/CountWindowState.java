public class CountWindowState {
    
    // window start time (event time), in milliseconds
    private long window_start_ms;

    // current count within the window
    private int count;

    // last seen event time (event time), in milliseconds
    private long last_event_ms;

    public CountWindowState() {}

    public long getWindow_start_ms() { return window_start_ms; }
    public void setWindow_start_ms(long window_start_ms) { this.window_start_ms = window_start_ms; }

    public int getCount() { return count; }
    public void setCount(int count) { this.count = count; }

    public long getLast_event_ms() { return last_event_ms; }
    public void setLast_event_ms(long last_event_ms) { this.last_event_ms = last_event_ms; }
    
}
