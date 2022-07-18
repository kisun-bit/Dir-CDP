package logic

import (
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"jingrongshuan/rongan-fnotify/meta"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"strings"
)

func UploadFile2Host(url string, fp *os.File, target string, mode os.FileMode) (err error) {
	r, w := io.Pipe()
	writer := multipart.NewWriter(w)
	defer r.Close()

	// FIX: 优化大文件传输时内存占用过大的问题
	go func() {
		defer w.Close()
		defer writer.Close()

		if err = writer.WriteField("mode", strconv.FormatInt(int64(mode), 10)); err != nil {
			logger.Fmt.Warnf("UploadFile2Host writer.WriteField ERR=%v", err)
			return
		}
		var part io.Writer
		part, err = writer.CreateFormFile("filename", target)
		if err != nil {
			logger.Fmt.Warnf("UploadFile2Host writer.CreateFormFile ERR=%v", err)
			return
		}
		if _, err = io.Copy(part, fp); err != nil {
			logger.Fmt.Warnf("UploadFile2Host io.Copy ERR=%v", err)
			return
		}
	}()

	req, err := http.NewRequest(http.MethodPost, url, r)
	if err != nil {
		logger.Fmt.Warnf("UploadFile2Host http.NewRequest ERR=%v", err)
		return err
	}
	req.Header.Add("Content-Type", writer.FormDataContentType())
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: meta.Pool, InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}
	req.Close = true
	resp, err := client.Do(req)
	if err != nil {
		if strings.Contains(err.Error(), "EOF") {
			logger.Fmt.Warnf("UploadFile2Host upload `%v` POST EOF", fp.Name())
			return nil
		}
		logger.Fmt.Warnf("UploadFile2Host client.Do ERR=%v", err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	bb, _ := ioutil.ReadAll(resp.Body)
	return fmt.Errorf("invalid status(%v) reason(%v)", resp.StatusCode, string(bb))
}

func RequestUrl(method, url string, FormData map[string]string) (body []byte, err error) {
	var form http.Request
	if err = form.ParseForm(); err != nil {
		return
	}
	for k, v := range FormData {
		form.Form.Add(k, v)
	}
	reqBody := strings.TrimSpace(form.Form.Encode())
	req, err := http.NewRequest(method, url, strings.NewReader(reqBody))
	if err != nil {
		logger.Fmt.Warnf("Request.NewRequest ERR=%v", err)
		return body, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Close = true
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: meta.Pool, InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}
	resp, err := client.Do(req)
	if err != nil {
		logger.Fmt.Warnf("Request.Client.Do ERR=%v", err)
		return body, err
	}
	defer resp.Body.Close()
	bs, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.Fmt.Warnf("Request.ReadAll ERR=%v", err)
		return body, err
	}
	if resp.StatusCode != http.StatusOK {
		logger.Fmt.Warnf("Request http returns `%s`", string(bs))
		return body, fmt.Errorf("request failed")
	}
	return bs, nil
}
