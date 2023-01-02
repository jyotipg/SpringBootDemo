	private String removeSpecificChars(String originalstring, String datType) {
		String removecharacterstring = "";
		if (datType.toLowerCase().trim().equals("keyword")) {
			removecharacterstring = "~!@#$%^&*()-_+=|\\?//><.,;:''{}\\\"";
		}
		if (datType.toLowerCase().trim().equals("integer")) {
			removecharacterstring = "~!@#$%^&*()-_+=|\\?//><.,;:''{}\\\\\\\"";
		}
		if (datType.toLowerCase().trim().equals("string") ) {
			return trim(originalstring);
		}
		if (datType.toLowerCase().trim().equals("float") || datType.toLowerCase().trim().equals("double")) {
			removecharacterstring = "~!@#$%^&*()-_+=|\\?//><,;:''{}\\\\\\\"";
		}
		if (datType.toLowerCase().trim().equals("date")) {
			String text = originalstring;

			text = text.replaceAll("/", "-");
			text = text.replaceAll("[^a-zA-Z0-9\\\\s+^-]", "");
			text = text.replaceAll("\\\\", "");
			text = text.replaceAll("/", "-");
			return text.replaceAll("--", "-").trim();
		}
		if (datType.toLowerCase().trim().equals("time")) {
			removecharacterstring = "~!@#$%^&*()-_+=|\\?//><.,;''{}\\\\\\\\\\\\\\\"";
		}
		char[] orgchararray = originalstring.toCharArray();
		char[] removechararray = removecharacterstring.toCharArray();
		int start, end = 0;
		boolean[] tempBoolean = new boolean[128];
		for (start = 0; start < removechararray.length; ++start) {
			tempBoolean[removechararray[start]] = true;
		}
		for (start = 0; start < orgchararray.length; ++start) {
			if (!tempBoolean[orgchararray[start]]) {
				orgchararray[end++] = orgchararray[start];
			}
		}

		return new String(orgchararray, 0, end).trim();
	}
	
	private String trim(String value)
	{
		String  specialCharacter="`~!@#$%^&*()_+-=|\\][{}:;<>,./?'\"";
		String result=value;
		for (char c:specialCharacter.toCharArray())
		{  			
			result=trim(result,c);
		}		
		return result;
		
	}

	private String trim(String value, char c) {

    if (c <= 32) return value.trim();
    int len = value.length();
    int st = 0;
    char[] val = value.toCharArray();     

    while ((st < len) && (val[st] == c)) {
        st++;
    }
    while ((st < len) && (val[len - 1] == c)) {
        len--;
    }
    return ((st > 0) || (len < value.length())) ? value.substring(st, len) : value;
   } 	
