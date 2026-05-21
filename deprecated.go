package pq

// ParseURL converts a url to a connection string for driver.Open.
//
// Deprecated: directly passing an URL to sql.Open("postgres", "postgres://...")
// now works, and calling this manually is no longer required.
func ParseURL(url string) (string, error) { return convertURL(url) }
