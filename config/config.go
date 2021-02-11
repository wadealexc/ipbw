package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/urfave/cli/v2"
	"github.com/wadeAlexC/ipbw/crawler"
	"github.com/wadeAlexC/ipbw/crawler/modules"
	"go.uber.org/fx"
)

type Config struct {
	Modules []fx.Option
	Invokes []fx.Option

	hello []string
}

const (
	// config
	FlagConfigFile = "c"

	// crawler
	FlagNumWorkers    = "w"
	FlagCrawlDuration = "d"

	// modules/status
	FlagEnableStatus   = "s"
	FlagStatusInterval = "status-interval"

	// modules/identifier
	FlagEnableIdentifier   = "i"
	FlagIdentifierInterval = "identifier-interval"

	// modules/reporter
	FlagEnableReporter        = "r"
	FlagReportInterval        = "report-interval"
	FlagReportPublishEndpoint = "report-endpoint"
	FlagReportPingEndpoint    = "report-ping"
	FlagReportAPIKey          = "report-api-key"
)

var Flags = []cli.Flag{
	&cli.StringFlag{
		Name:  FlagConfigFile,
		Usage: "load config from `FILE`, ignoring any other set flags",
	},
	&cli.UintFlag{
		Name:  FlagCrawlDuration,
		Value: 0,
		Usage: "specify `NUM_MINUTES` to run the crawler. 0 will run in endless mode.",
	},
	&cli.UintFlag{
		Name:  FlagNumWorkers,
		Value: 8,
		Usage: "specify the number of goroutines used to query the DHT",
	},
	&cli.BoolFlag{
		Name:    FlagEnableStatus,
		Aliases: []string{"status"},
		Usage:   "specify whether the crawler should output occasional status updates to console.",
	},
	&cli.UintFlag{
		Name:   FlagStatusInterval,
		Value:  1,
		Hidden: true,
		Usage:  "specify `NUM_MINUTES` between status updates to console",
	},
	&cli.BoolFlag{
		Name:    FlagEnableIdentifier,
		Aliases: []string{"identifier"},
		Usage:   "enables the identifier module",
	},
	&cli.UintFlag{
		Name:    FlagIdentifierInterval,
		Aliases: []string{"ii"},
		Value:   1,
		Hidden:  true,
		Usage:   "specify `NUM_MINUTES` between identifier output",
	},
	&cli.BoolFlag{
		Name:    FlagEnableReporter,
		Aliases: []string{"reporter"},
		Usage:   "enables the reporter module, which publishes results to a server",
	},
	&cli.UintFlag{
		Name:    FlagReportInterval,
		Aliases: []string{"ri"},
		Value:   1,
		Hidden:  true,
		Usage:   "how often (in minutes) the reporter will publish crawl results to the server",
	},
	&cli.StringFlag{
		Name:    FlagReportPublishEndpoint,
		Aliases: []string{"re"},
		Value:   "http://127.0.0.1:8000/batch",
		Hidden:  true,
		Usage:   "the url/endpoint the reporter will publish crawl results to",
	},
	&cli.StringFlag{
		Name:    FlagReportPingEndpoint,
		Aliases: []string{"rp"},
		Value:   "http://127.0.0.1:8000/healthcheck",
		Hidden:  true,
		Usage:   "the url/endpoint the reporter will ping on startup to ensure the server is running",
	},
	&cli.StringFlag{
		Name:    FlagReportAPIKey,
		Aliases: []string{"a"},
		Hidden:  true,
		Usage:   "the API key used to authenticate the reporter's reports",
	},
}

// Get creates an app config from a file or from CLI flags
func Get(cctx *cli.Context) (*Config, error) {

	// Check if we were provided a config file:
	var config *Config
	if cctx.IsSet(FlagConfigFile) {
		var err error
		fileName := cctx.String(FlagConfigFile)
		config, err = fromFile(fileName)
		if err != nil {
			return nil, fmt.Errorf("Error reading from config file: %v", err)
		}

		return config, err
	} else {
		return fromFlags(cctx), nil
	}
}

type configJSON struct {
	NumWorkers    uint         `json:"numWorkers"`
	CrawlDuration uint         `json:"crawlDuration"`
	Modules       *modulesJSON `json:"modules"`
}

type modulesJSON struct {
	Status     *statusJSON     `json:"status"`
	Reporter   *reporterJSON   `json:"reporter"`
	Identifier *identifierJSON `json:"identifier"`
}

type statusJSON struct {
	Interval uint `json:"interval"`
}

type reporterJSON struct {
	Interval        uint   `json:"interval"`
	PublishEndpoint string `json:"publishEndpoint"`
	PingEndpoint    string `json:"pingEndpoint"`
	ApiKey          string `json:"apiKey"`
}

type identifierJSON struct {
	Interval uint `json:"interval"`
}

// fromFile creates a crawler config from the provided json file
func fromFile(fileName string) (*Config, error) {

	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("Error opening file: %v", err)
	}

	var cfgJSON *configJSON
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields() // error on unknown fields
	err = decoder.Decode(&cfgJSON)
	if err != nil {
		return nil, fmt.Errorf("Error decoding JSON: %v", err)
	}

	var durationString string
	if cfgJSON.CrawlDuration == 0 {
		durationString = fmt.Sprintf("crawl duration: endless\n")
	} else {
		durationString = fmt.Sprintf("crawl duration: %d min\n", cfgJSON.CrawlDuration)
	}

	// defaults
	c := &Config{
		Modules: []fx.Option{fx.Provide(crawler.NewCrawler)},
		Invokes: []fx.Option{fx.Invoke(func(c *crawler.Crawler) error {
			return c.Setup(cfgJSON.NumWorkers, cfgJSON.CrawlDuration)
		})},
		hello: []string{
			fmt.Sprintf("IPBW - starting crawl with config from %s:\n", fileName),
			"====================\n",
			fmt.Sprintf("number of workers: %d\n", cfgJSON.NumWorkers),
			durationString,
			"====================\n",
		},
	}

	if cfgJSON.Modules == nil {
		return c, nil
	}

	// modules/status
	if cfgJSON.Modules.Status != nil {
		c.Modules = append(c.Modules, fx.Provide(modules.NewStatus))
		c.Invokes = append(c.Invokes, fx.Invoke(func(s *modules.Status) error {
			return s.Setup(cfgJSON.Modules.Status.Interval)
		}))
		c.hello = append(c.hello,
			"modules/status: enabled\n",
			fmt.Sprintf("- status interval: %d min\n", cfgJSON.Modules.Status.Interval),
		)
	}

	// modules/reporter
	if cfgJSON.Modules.Reporter != nil {
		c.Modules = append(c.Modules, fx.Provide(modules.NewReporter))
		c.Invokes = append(c.Invokes, fx.Invoke(func(r *modules.Reporter) error {
			return r.Setup(
				cfgJSON.Modules.Reporter.Interval,
				cfgJSON.Modules.Reporter.PublishEndpoint,
				cfgJSON.Modules.Reporter.PingEndpoint,
				cfgJSON.Modules.Reporter.ApiKey,
			)
		}))
		c.hello = append(c.hello,
			"modules/reporter: enabled\n",
			fmt.Sprintf("- report interval: %d min\n", cfgJSON.Modules.Reporter.Interval),
			fmt.Sprintf("- report publish endpoint: %s\n", cfgJSON.Modules.Reporter.PublishEndpoint),
			fmt.Sprintf("- report ping endpoint: %s\n", cfgJSON.Modules.Reporter.PingEndpoint),
			fmt.Sprintf("- report api key: %s\n", cfgJSON.Modules.Reporter.ApiKey),
		)
	}

	// modules/identifier
	if cfgJSON.Modules.Identifier != nil {
		c.Modules = append(c.Modules, fx.Provide(modules.NewIdentifier))
		c.Invokes = append(c.Invokes, fx.Invoke(func(i *modules.Identifier) error {
			return i.Setup(cfgJSON.Modules.Identifier.Interval)
		}))
		c.hello = append(c.hello,
			"modules/identifier: enabled\n",
			fmt.Sprintf("- identifier interval: %d min\n", cfgJSON.Modules.Identifier.Interval),
		)
	}

	return c, nil
}

// fromFlags creates a crawler config from CLI flags
func fromFlags(cctx *cli.Context) *Config {
	config := defaults(cctx)
	config.configStatus(cctx)
	config.configIdentifier(cctx)
	config.configReporter(cctx)
	return config
}

// defaults returns a Config that sets up the crawler
// - Provide the NewCrawler constructor to the app
// - Invoke SetNumWorkers on app startup
func defaults(cctx *cli.Context) *Config {

	var durationString string
	if cctx.Uint(FlagCrawlDuration) == 0 {
		durationString = fmt.Sprintf("crawl duration: endless\n")
	} else {
		durationString = fmt.Sprintf("crawl duration: %d min\n", cctx.Uint(FlagCrawlDuration))
	}

	config := &Config{
		Modules: []fx.Option{fx.Provide(crawler.NewCrawler)},
		Invokes: []fx.Option{fx.Invoke(func(c *crawler.Crawler) error {
			return c.Setup(cctx.Uint(FlagNumWorkers), cctx.Uint(FlagCrawlDuration))
		})},
		hello: []string{
			"IPBW - starting crawl with config:\n",
			"====================\n",
			fmt.Sprintf("number of workers: %d\n", cctx.Uint(FlagNumWorkers)),
			durationString,
			"====================\n",
		},
	}

	return config
}

// configStatus adds setup for modules/status, if enabled
func (c *Config) configStatus(cctx *cli.Context) {
	if cctx.Bool(FlagEnableStatus) {
		c.Modules = append(c.Modules, fx.Provide(modules.NewStatus))
		c.Invokes = append(c.Invokes, fx.Invoke(func(s *modules.Status) error {
			return s.Setup(cctx.Uint(FlagStatusInterval))
		}))
		c.hello = append(c.hello,
			"modules/status: enabled\n",
			fmt.Sprintf("- status interval: %d min\n", cctx.Uint(FlagStatusInterval)),
		)
	}
}

// configIdentifier adds setup for modules/identifier, if enabled
func (c *Config) configIdentifier(cctx *cli.Context) {
	if cctx.Bool(FlagEnableIdentifier) {
		c.Modules = append(c.Modules, fx.Provide(modules.NewIdentifier))
		c.Invokes = append(c.Invokes, fx.Invoke(func(i *modules.Identifier) error {
			return i.Setup(cctx.Uint(FlagIdentifierInterval))
		}))
		c.hello = append(c.hello,
			"modules/identifier: enabled\n",
			fmt.Sprintf("- identifier interval: %d min\n", cctx.Uint(FlagIdentifierInterval)),
		)
	}
}

func (c *Config) configReporter(cctx *cli.Context) {
	if cctx.Bool(FlagEnableReporter) {
		c.Modules = append(c.Modules, fx.Provide(modules.NewReporter))
		c.Invokes = append(c.Invokes, fx.Invoke(func(r *modules.Reporter) error {
			return r.Setup(
				cctx.Uint(FlagReportInterval),
				cctx.String(FlagReportPublishEndpoint),
				cctx.String(FlagReportPingEndpoint),
				cctx.String(FlagReportAPIKey),
			)
		}))
		c.hello = append(c.hello,
			"modules/reporter: enabled\n",
			fmt.Sprintf("- report interval: %d min\n", cctx.Uint(FlagReportInterval)),
			fmt.Sprintf("- report publish endpoint: %s\n", cctx.String(FlagReportPublishEndpoint)),
			fmt.Sprintf("- report ping endpoint: %s\n", cctx.String(FlagReportPingEndpoint)),
			fmt.Sprintf("- report api key: %s\n", cctx.String(FlagReportAPIKey)),
		)
	}
}

// PrintHello prints a summary of config to the console
func (c *Config) PrintHello() {
	hello := strings.Join(c.hello, "")
	fmt.Printf(hello)
	fmt.Printf("====================\n")
}
