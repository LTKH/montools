package main

import (
    //"crypto/tls"
    "crypto/x509"
    "encoding/pem"
    "io/ioutil"
    "log"
    //"net/http"
    "fmt"
)  
   
func main() {
    privateKeyBytes, err := ioutil.ReadFile("config/mtproxy.key")
    if err != nil {
        log.Fatalf("failed to read private key file: %v", err)
    }

    block, _ := pem.Decode(privateKeyBytes)
    if err != nil {
        log.Fatalf("failed to decode private key to PEM: %v", err)
    }

    fmt.Printf("private key PEM type: %s\n", block.Type)

    //decryptedPrivateKeyBytes, err := x509.DecryptPEMBlock(block, []byte("mysupersecretpassword"))
    //if err != nil {
    //    log.Fatalf("failed to decrypt private key: %v", err)
    //}
    //fmt.Printf("decoded private key %s\n", string(decryptedPrivateKeyBytes))

    ecdsaPrivateKey, err := x509.ParseECPrivateKey(block.Bytes)
    if err != nil {
        log.Fatalf("failed to parse PEM block EC private key: %v", err)
    }

    privateKeyDERBytes, err := x509.MarshalECPrivateKey(ecdsaPrivateKey)
    if err != nil {
        log.Fatalf("failed to marshal EC private key to DER: %v", err)
    }

    keyBlock := &pem.Block{
        Type:  "EC PRIVATE KEY",
        Bytes: privateKeyDERBytes,
    }

    privateKeyBytes = pem.EncodeToMemory(keyBlock)

    fmt.Printf("decoded private key %s\n", string(privateKeyBytes))
}