package logger

type (
	BlueString      string
	CyanString      string
	GreenString     string
	GreenBoldString string
	MagentaString   string
	RedString       string
	YellowString    string

	ColoredString interface {
		String() string
	}
)

const (
	colorBlue      = "\x1b[34m"
	colorCyan      = "\x1b[96m"
	colorGreen     = "\x1b[92m"
	colorGreenBold = "\x1b[92;1m"
	colorMagenta   = "\x1b[35m"
	colorRed       = "\x1b[31m"
	colorYellow    = "\x1b[33m"
	colorReset     = "\x1b[0m"
)

func (s BlueString) String() string {
	return colorBlue + string(s) + colorReset
}

func (s CyanString) String() string {
	return colorCyan + string(s) + colorReset
}

func (s GreenString) String() string {
	return colorGreen + string(s) + colorReset
}

func (s GreenBoldString) String() string {
	return colorGreenBold + string(s) + colorReset
}

func (s MagentaString) String() string {
	return colorMagenta + string(s) + colorReset
}

func (s RedString) String() string {
	return colorRed + string(s) + colorReset
}

func (s YellowString) String() string {
	return colorYellow + string(s) + colorReset
}
