package main

import (
	"log"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/masonictemple4/proglog/internal/agent"
	"github.com/masonictemple4/proglog/internal/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type cli struct {
	cfg cfg
}

type cfg struct {
	agent.Config
	ServerTLSConfig config.TLSConfig
	PeerTLSConfig   config.TLSConfig
}

func main() {

	cli := &cli{}

	root := &cobra.Command{
		Use:     "proglog",
		Short:   "Distributed log",
		Long:    "Distributed log",
		PreRunE: cli.setupConfig,
		RunE:    cli.run,
	}

	if err := setupFlags(root); err != nil {
		log.Fatal(err)
	}

	if err := root.Execute(); err != nil {
		log.Fatal(err)
	}

}

func setupFlags(root *cobra.Command) error {
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	root.Flags().String("config-file", "", "Path to config file")
	dataDir := path.Join(os.TempDir(), "proglog")
	root.Flags().String("data-dir", dataDir, "Directory to store log and raft data.")
	root.Flags().String("node-name", hostname, "Unique server ID")

	root.Flags().String("bind-addr", "127.0.0.1:8401", "Address to bind Serf on.")
	root.Flags().Int("rpc-port", 8400, "Port for RPC clients (and Raft) connections.")
	root.Flags().StringSlice("start-join-addrs", nil, "Serf addresses to join.")
	root.Flags().Bool("bootstrap", false, "Bootstrap the cluster.")

	root.Flags().String("acl-model-file", "", "Path to ACL model.")
	root.Flags().String("acl-policy-file", "", "Path to ACL policy.")

	root.Flags().String("server-tls-cert-file", "", "Path to server tls cert.")
	root.Flags().String("server-tls-key-file", "", "Path to server tls key.")
	root.Flags().String("server-tls-ca-file", "", "Path to server certificate authority file.")

	root.Flags().String("peer-tls-cert-file", "", "Path to peer tls cert.")
	root.Flags().String("peer-tls-key-file", "", "Path to peer tls key.")
	root.Flags().String("peer-tls-ca-file", "", "Path to peer certificate authority file.")

	return viper.BindPFlags(root.Flags())
}

func (c *cli) setupConfig(cmd *cobra.Command, args []string) error {

	configFile, err := cmd.Flags().GetString("config-file")
	if err != nil {
		return err
	}
	viper.SetConfigFile(configFile)

	if err = viper.ReadInConfig(); err != nil {
		// for any other error than not found return err.
		// otherwise just do nothing its ok if no file is
		// provided.
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return err
		}
	}

	c.cfg.DataDir = viper.GetString("data-dir")
	c.cfg.NodeName = viper.GetString("node-name")
	c.cfg.BindAddr = viper.GetString("bind-addr")
	c.cfg.RPCPort = viper.GetInt("rpc-port")
	c.cfg.StartJoinAddrs = viper.GetStringSlice("start-join-addrs")
	c.cfg.Bootstrap = viper.GetBool("bootstrap")
	c.cfg.ACLModelFile = viper.GetString("acl-model-file")
	c.cfg.ACLPolicyFile = viper.GetString("acl-policy-file")
	c.cfg.ServerTLSConfig.CertFile = viper.GetString("server-tls-cert-file")
	c.cfg.ServerTLSConfig.KeyFile = viper.GetString("server-tls-key-file")
	c.cfg.ServerTLSConfig.CAFile = viper.GetString("server-tls-ca-file")
	c.cfg.PeerTLSConfig.CertFile = viper.GetString("peer-tls-cert-file")
	c.cfg.PeerTLSConfig.KeyFile = viper.GetString("peer-tls-key-file")
	c.cfg.PeerTLSConfig.CAFile = viper.GetString("peer-tls-ca-file")

	if c.cfg.ServerTLSConfig.CertFile != "" && c.cfg.ServerTLSConfig.KeyFile != "" {
		c.cfg.ServerTLSConfig.Server = true
		c.cfg.Config.ServerTLSConfig, err = config.SetupTLSConfig(c.cfg.ServerTLSConfig)
		if err != nil {
			return err
		}
	}

	if c.cfg.PeerTLSConfig.CertFile != "" && c.cfg.PeerTLSConfig.KeyFile != "" {
		c.cfg.Config.PeerTLSConfig, err = config.SetupTLSConfig(c.cfg.PeerTLSConfig)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *cli) run(cmd *cobra.Command, args []string) error {
	agent, err := agent.New(c.cfg.Config)
	if err != nil {
		return err
	}
	// Common pattern for managing shutdowns of long-running processes in go.
	// SIGINT: Ctrl+C
	// SIGTERM: termination signal, typically sent by system-level tools
	// to request a graceful shutdown.
	// Since we're listening for the signal and blocking until it's received,
	// this allows us to decide what and how we want to cleanup before exiting.
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	<-sigc
	return agent.Shutdown()
}
