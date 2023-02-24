package youtube

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
)

// Client offers methods to download video metadata and video streams.
type Client struct {
	// Debug enables debugging output through log package
	Debug bool

	// HTTPClient can be used to set a custom HTTP client.
	// If not set, http.DefaultClient will be used
	HTTPClient *http.Client

	// playerCache caches the JavaScript code of a player response
	playerCache playerCache
}

// GetVideo fetches video metadata
func (c *Client) GetVideo(url string) (*Video, error) {
	return c.GetVideoContext(context.Background(), url)
}

// GetVideoContext fetches video metadata with a context
func (c *Client) GetVideoContext(ctx context.Context, url string) (*Video, error) {
	id, err := ExtractVideoID(url)
	if err != nil {
		return nil, fmt.Errorf("extractVideoID failed: %w", err)
	}
	return c.videoFromID(ctx, id)
}

func (c *Client) videoFromID(ctx context.Context, id string) (*Video, error) {
	body, err := c.videoDataByInnertube(ctx, id, webClient)
	if err != nil {
		return nil, err
	}

	v := &Video{
		ID: id,
	}

	err = v.parseVideoInfo(body)
	// return early if all good
	if err == nil {
		return v, nil
	}

	// If the uploader has disabled embedding the video on other sites, parse video page
	if err == ErrNotPlayableInEmbed {
		// additional parameters are required to access clips with sensitiv content
		html, err := c.httpGetBodyBytes(ctx, "https://www.youtube.com/watch?v="+id+"&bpctr=9999999999&has_verified=1")
		if err != nil {
			return nil, err
		}

		return v, v.parseVideoPage(html)
	}

	// If the uploader marked the video as inappropriate for some ages, use embed player
	if err == ErrLoginRequired {
		bodyEmbed, errEmbed := c.videoDataByInnertube(ctx, id, embeddedClient)
		if errEmbed == nil {
			errEmbed = v.parseVideoInfo(bodyEmbed)
		}

		if errEmbed == nil {
			return v, nil
		}

		// private video clearly not age-restricted and thus should be explicit
		if errEmbed == ErrVideoPrivate {
			return v, errEmbed
		}

		// wrapping error so its clear whats happened
		return v, fmt.Errorf("can't bypass age restriction: %w", errEmbed)
	}

	// undefined error
	return v, err
}

type innertubeRequest struct {
	VideoID         string            `json:"videoId,omitempty"`
	BrowseID        string            `json:"browseId,omitempty"`
	Continuation    string            `json:"continuation,omitempty"`
	Context         inntertubeContext `json:"context"`
	PlaybackContext playbackContext   `json:"playbackContext,omitempty"`
}

type playbackContext struct {
	ContentPlaybackContext contentPlaybackContext `json:"contentPlaybackContext"`
}

type contentPlaybackContext struct {
	SignatureTimestamp string `json:"signatureTimestamp"`
}

type inntertubeContext struct {
	Client innertubeClient `json:"client"`
}

type innertubeClient struct {
	HL            string `json:"hl"`
	GL            string `json:"gl"`
	ClientName    string `json:"clientName"`
	ClientVersion string `json:"clientVersion"`
}

// client info for the innertube API
type clientInfo struct {
	name    string
	key     string
	version string
}

var (
	// might add ANDROID and other in future, but i don't see reason yet
	webClient = clientInfo{
		name:    "WEB",
		version: "2.20210617.01.00",
		key:     "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8",
	}

	embeddedClient = clientInfo{
		name:    "WEB_EMBEDDED_PLAYER",
		version: "1.19700101",
		key:     "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8", // seems like same key works for both clients
	}
)

func (c *Client) videoDataByInnertube(ctx context.Context, id string, clientInfo clientInfo) ([]byte, error) {
	config, err := c.getPlayerConfig(ctx, id)
	if err != nil {
		return nil, err
	}

	// fetch sts first
	sts, err := config.getSignatureTimestamp()
	if err != nil {
		return nil, err
	}

	context := prepareInnertubeContext(clientInfo)

	data := innertubeRequest{
		VideoID: id,
		Context: context,
		PlaybackContext: playbackContext{
			ContentPlaybackContext: contentPlaybackContext{
				SignatureTimestamp: sts,
			},
		},
	}

	return c.httpPostBodyBytes(ctx, "https://www.youtube.com/youtubei/v1/player?key="+clientInfo.key, data)
}

func prepareInnertubeContext(clientInfo clientInfo) inntertubeContext {
	return inntertubeContext{
		Client: innertubeClient{
			HL:            "en",
			GL:            "US",
			ClientName:    clientInfo.name,
			ClientVersion: clientInfo.version,
		},
	}
}

func prepareInnertubePlaylistData(ID string, continuation bool, clientInfo clientInfo) innertubeRequest {
	context := prepareInnertubeContext(clientInfo)

	if continuation {
		return innertubeRequest{Context: context, Continuation: ID}
	}

	return innertubeRequest{Context: context, BrowseID: "VL" + ID}
}

// GetPlaylist fetches playlist metadata
func (c *Client) GetPlaylist(url string) (*Playlist, error) {
	return c.GetPlaylistContext(context.Background(), url)
}

// GetPlaylistContext fetches playlist metadata, with a context, along with a list of Videos, and some basic information
// for these videos. Playlist entries cannot be downloaded, as they lack all the required metadata, but
// can be used to enumerate all IDs, Authors, Titles, etc.
func (c *Client) GetPlaylistContext(ctx context.Context, url string) (*Playlist, error) {
	id, err := extractPlaylistID(url)
	if err != nil {
		return nil, fmt.Errorf("extractPlaylistID failed: %w", err)
	}

	data := prepareInnertubePlaylistData(id, false, webClient)
	body, err := c.httpPostBodyBytes(ctx, "https://www.youtube.com/youtubei/v1/browse?key="+webClient.key, data)
	if err != nil {
		return nil, err
	}

	p := &Playlist{ID: id}
	return p, p.parsePlaylistInfo(ctx, c, body)
}

func (c *Client) VideoFromPlaylistEntry(entry *PlaylistEntry) (*Video, error) {
	return c.videoFromID(context.Background(), entry.ID)
}

func (c *Client) VideoFromPlaylistEntryContext(ctx context.Context, entry *PlaylistEntry) (*Video, error) {
	return c.videoFromID(ctx, entry.ID)
}

// GetStream returns the stream and the total size for a specific format
func (c *Client) GetStream(video *Video, format *Format) (io.ReadCloser, int64, error) {
	return c.GetStreamContext(context.Background(), video, format)
}

// GetStreamContext returns the stream and the total size for a specific format with a context.
func (c *Client) GetStreamContext1(ctx context.Context, video *Video, format *Format, byteUpdate chan int64, done chan bool) (io.ReadCloser, int64, error) {
	url, err := c.GetStreamURL(video, format)
	if err != nil {
		return nil, 0, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}

	r, w := io.Pipe()
	contentLength := format.ContentLength

	if contentLength == 0 {
		// some videos don't have length information
		contentLength = c.downloadOnce(req, w, format)
	} else {
		// we have length information, let's download by chunks!
		go c.downloadChunked1(func() *http.Request {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				panic("failed to create request: " + err.Error())
			}
			return req
		}, w, format, byteUpdate, video.ID, done)
	}

	return r, contentLength, nil
}

// GetStreamContext returns the stream and the total size for a specific format with a context.
func (c *Client) GetStreamContext(ctx context.Context, video *Video, format *Format) (io.ReadCloser, int64, error) {
	url, err := c.GetStreamURL(video, format)
	if err != nil {
		return nil, 0, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}

	r, w := io.Pipe()
	contentLength := format.ContentLength

	if contentLength == 0 {
		// some videos don't have length information
		contentLength = c.downloadOnce(req, w, format)
	} else {
		// we have length information, let's download by chunks!
		go c.downloadChunked(req, w, format)
	}

	return r, contentLength, nil
}

func (c *Client) downloadOnce(req *http.Request, w *io.PipeWriter, format *Format) int64 {
	resp, err := c.httpDo(req)
	if err != nil {
		//nolint:errcheck
		w.CloseWithError(err)
		return 0
	}

	go func() {
		defer resp.Body.Close()
		_, err := io.Copy(w, resp.Body)
		if err == nil {
			w.Close()
		} else {
			//nolint:errcheck
			w.CloseWithError(err)
		}
	}()

	contentLength := resp.Header.Get("Content-Length")
	len, _ := strconv.ParseInt(contentLength, 10, 64)

	return len
}

type Pair struct {
	Byte  int64
	Err   error
	Index int
}

type PassThru struct {
	io.Reader
	total     int64 // Total # of bytes transferred
	bytesChan chan int64
}

func (pt *PassThru) Read(p []byte) (int, error) {
	n, err := pt.Reader.Read(p)
	b := int64(n)
	pt.bytesChan <- b
	pt.total += b

	//if err == nil {
	//	fmt.Println("Read", n, "bytes for a total of", pt.total)
	//}

	return n, err
}

func (c *Client) downloadChunked1(reqFunc func() *http.Request, w *io.PipeWriter, format *Format, byteUpdate chan int64,
	filename string, done chan bool,
) {
	const chunkSize int64 = 5_000_000
	// Loads a chunk a returns the written bytes.
	// Downloading in multiple chunks is much faster:
	// https://github.com/kkdai/youtube/pull/190
	var wg sync.WaitGroup
	var pairs = make([]Pair, 0)

	loadChunk := func(pos int64, index int) {
		req := reqFunc()
		req.Header.Set("Range", fmt.Sprintf("bytes=%v-%v", pos, pos+chunkSize-1))

		resp, err := c.httpDo(req)
		if err != nil {
			pairs = append(pairs, Pair{Byte: 0, Err: err, Index: index})
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusPartialContent {
			pairs = append(pairs, Pair{0, ErrUnexpectedStatusCode(resp.StatusCode), index})
			return
		}
		f, err := os.Create(filename + ".temp_" + strconv.Itoa(index))
		if err != nil {
			pairs = append(pairs, Pair{Byte: 0, Err: err, Index: index})
			return
		}
		defer f.Close()
		writer := bufio.NewWriter(f)
		p := PassThru{bytesChan: byteUpdate, Reader: resp.Body}

		count, err := io.Copy(writer, &p)
		pairs = append(pairs, Pair{Byte: count, Err: err, Index: index})
		log.Println("Done item: ", index, "")
		wg.Done()
	}

	defer w.Close()

	//nolint:revive,errcheck
	// load all the chunks
	var index = 0
	for pos := int64(0); pos < format.ContentLength; {
		wg.Add(1)
		go loadChunk(pos, index)
		pos += chunkSize
		index += 1
	}
	wg.Wait()
	fmt.Println("Start merge file")

	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Index < pairs[j].Index
	})
	for _, pairs := range pairs {
		if pairs.Err != nil {
			panic("error in downloadChunked1")
		}

		f, err := os.Open(filename + ".temp_" + strconv.Itoa(pairs.Index))
		if err != nil {
			panic("error in downloadChunked1")
		}
		_, err = io.Copy(w, bufio.NewReader(f))
		if err != nil {
			panic("error in copy bytes")
		}
		f.Close()
		err = os.Remove(f.Name())
		if err != nil {
			log.Println(err.Error())
			log.Println("error in remove file")
		}
	}
	done <- true
}
func (c *Client) downloadChunked(req *http.Request, w *io.PipeWriter, format *Format) {
	const chunkSize int64 = 10_000_000
	// Loads a chunk a returns the written bytes.
	// Downloading in multiple chunks is much faster:
	// https://github.com/kkdai/youtube/pull/190
	loadChunk := func(pos int64) (int64, error) {
		req.Header.Set("Range", fmt.Sprintf("bytes=%v-%v", pos, pos+chunkSize-1))

		resp, err := c.httpDo(req)
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusPartialContent {
			return 0, ErrUnexpectedStatusCode(resp.StatusCode)
		}

		return io.Copy(w, resp.Body)
	}

	defer w.Close()

	//nolint:revive,errcheck
	// load all the chunks
	for pos := int64(0); pos < format.ContentLength; {
		written, err := loadChunk(pos)
		if err != nil {
			w.CloseWithError(err)
			return
		}

		pos += written
	}
}

// GetStreamURL returns the url for a specific format
func (c *Client) GetStreamURL(video *Video, format *Format) (string, error) {
	return c.GetStreamURLContext(context.Background(), video, format)
}

// GetStreamURLContext returns the url for a specific format with a context
func (c *Client) GetStreamURLContext(ctx context.Context, video *Video, format *Format) (string, error) {
	if format.URL != "" {
		return c.unThrottle(ctx, video.ID, format.URL)
	}

	cipher := format.Cipher
	if cipher == "" {
		return "", ErrCipherNotFound
	}

	uri, err := c.decipherURL(ctx, video.ID, cipher)
	if err != nil {
		return "", err
	}

	return uri, err
}

// httpDo sends an HTTP request and returns an HTTP response.
func (c *Client) httpDo(req *http.Request) (*http.Response, error) {
	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	if c.Debug {
		log.Println(req.Method, req.URL)
	}

	res, err := client.Do(req)

	if c.Debug && res != nil {
		log.Println(res.Status)
	}

	return res, err
}

// httpGet does a HTTP GET request, checks the response to be a 200 OK and returns it
func (c *Client) httpGet(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpDo(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, ErrUnexpectedStatusCode(resp.StatusCode)
	}
	return resp, nil
}

// httpGetBodyBytes reads the whole HTTP body and returns it
func (c *Client) httpGetBodyBytes(ctx context.Context, url string) ([]byte, error) {
	resp, err := c.httpGet(ctx, url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

// httpPost does a HTTP POST request with a body, checks the response to be a 200 OK and returns it
func (c *Client) httpPost(ctx context.Context, url string, body interface{}) (*http.Response, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	resp, err := c.httpDo(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, ErrUnexpectedStatusCode(resp.StatusCode)
	}
	return resp, nil
}

// httpPostBodyBytes reads the whole HTTP body and returns it
func (c *Client) httpPostBodyBytes(ctx context.Context, url string, body interface{}) ([]byte, error) {
	resp, err := c.httpPost(ctx, url, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}
