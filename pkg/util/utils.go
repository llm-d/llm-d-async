package util

// Make sure it starts with '/'
func NormalizeURLPath(path string) string {
	if len(path) == 0 {
		return "/"
	}
	if path[0] != '/' {
		path = "/" + path
	}
	return path
}

func NormalizeBaseURL(baseURL string) string {
	if len(baseURL) == 0 {
		return ""
	}

	//convert to loop
	for baseURL[len(baseURL)-1] == '/' {
		baseURL = baseURL[:len(baseURL)-1]
	}
	return baseURL

}
