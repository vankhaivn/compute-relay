package sqlite

// The only production driver. No external sqlite process, CGo fallback or test driver
// is selected at runtime. Pin its exact libc dependency together with the driver.
import _ "modernc.org/sqlite"
