package config

const (
	VerboseLevelNone    int = 0
	VerboseLevelMinimal int = 1
	VerboseLevelMedium  int = 2
	VerboseLevelMaximum int = 3

	RunErrorExecute int = 1
	RunErrorToken   int = 2
)

type RootOptions struct {
	ControlPlaneURL string
	VerboseLevel    int
	Version         string
	ServerToken     string
}

type RunOptions struct {
	ShowVersion bool
}
