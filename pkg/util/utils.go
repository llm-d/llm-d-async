package util

// Make sure it starts with '/'
func NormalizeURLPath(path string) string {
	if path[0] != '/' {
		path = "/" + path
	}
	return path
}

func NormalizeBaseURL(baseURL string) string {
	//convert to loop
	for baseURL[len(baseURL)-1] == '/' {
		baseURL = baseURL[:len(baseURL)-1]
	}
	return baseURL

}
