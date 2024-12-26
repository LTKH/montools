package config

import (
    "errors"
    "github.com/ltkh/montools/internal/config/mtproxy"
)

type Config interface {

}

func New(ctype, cfile string) (Config, error) {
    switch ctype {
        case "mtproxy":
            return mtproxy.NewConfig(cfile)
    }
    return nil, errors.New("invalid config type")
}