// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/ramendr/ramen/test/drenv-go/internal/addon"
)

type addonsReport struct {
	Addons []addonName `json:"addons"`
}

type addonName struct {
	Name string `json:"name"`
}

func newAddonsCommand() *cobra.Command {
	var asJSON bool

	cmd := &cobra.Command{
		Use:   "addons",
		Short: "List the addons implemented by drenv-go (no --envfile needed)",
		RunE: func(cmd *cobra.Command, args []string) error {
			names := addon.Names()
			if asJSON {
				r := addonsReport{Addons: make([]addonName, len(names))}
				for i, n := range names {
					r.Addons[i] = addonName{Name: n}
				}
				return writeJSON(cmd.OutOrStdout(), r)
			}
			for _, n := range names {
				fmt.Fprintln(cmd.OutOrStdout(), n)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "print a JSON document instead of text")
	return cmd
}
