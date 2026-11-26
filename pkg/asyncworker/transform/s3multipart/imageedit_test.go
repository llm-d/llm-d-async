package s3multipart

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	asyncapi "github.com/llm-d/llm-d-async/api"
)

const editsURL = "http://gw.example/prefix/v1/images/edits"

func TestPresignExpiry(t *testing.T) {
	whole := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	frac := whole.Add(900 * time.Millisecond) // SigV4 dates this as `whole`
	static := aws.Credentials{AccessKeyID: "k", SecretAccessKey: "s"}
	expiringAt := func(at time.Time) aws.Credentials {
		return aws.Credentials{AccessKeyID: "k", SecretAccessKey: "s", CanExpire: true, Expires: at}
	}
	tests := []struct {
		name     string
		now      time.Time
		deadline time.Time // zero: no deadline
		creds    aws.Credentials
		want     time.Duration
		wantOK   bool
	}{
		{name: "attempt deadline", now: whole, deadline: whole.Add(5 * time.Minute), creds: static, want: 5 * time.Minute, wantOK: true},
		{name: "fraction rounds up", now: whole, deadline: whole.Add(299*time.Second + 100*time.Millisecond), creds: static, want: 300 * time.Second, wantOK: true},
		// Signed at 12:00:00.9 → dated 12:00:00, so 1s would end at 12:00:01,
		// before the 12:00:01.3 deadline.
		{name: "counts from the whole-second signing time", now: frac, deadline: whole.Add(1300 * time.Millisecond), creds: static, want: 2 * time.Second, wantOK: true},
		{name: "far deadline clamps to 7 days", now: whole, deadline: whole.Add(30 * 24 * time.Hour), creds: static, want: 7 * 24 * time.Hour, wantOK: true},
		{name: "no deadline uses default", now: whole, creds: static, want: 15 * time.Minute, wantOK: true},
		{name: "ends a second before expiring credentials", now: frac, deadline: whole.Add(5 * time.Minute), creds: expiringAt(whole.Add(31 * time.Second)), want: 30 * time.Second, wantOK: true},
		{name: "credentials outlive deadline", now: whole, deadline: whole.Add(5 * time.Minute), creds: expiringAt(whole.Add(time.Hour)), want: 5 * time.Minute, wantOK: true},
		{name: "credentials about to expire", now: whole, deadline: whole.Add(5 * time.Minute), creds: expiringAt(whole.Add(1500 * time.Millisecond)), wantOK: false},
		{name: "exactly a second left is enough", now: whole, deadline: whole.Add(5 * time.Minute), creds: expiringAt(whole.Add(2 * time.Second)), want: time.Second, wantOK: true},
		{name: "deadline passed", now: whole, deadline: whole.Add(-time.Second), creds: static, wantOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := presignExpiry(tc.now, tc.deadline, !tc.deadline.IsZero(), tc.creds)
			if ok != tc.wantOK || (ok && got != tc.want) {
				t.Fatalf("presignExpiry = %v, %v; want %v, %v", got, ok, tc.want, tc.wantOK)
			}
			if !ok {
				return
			}
			// The URL is valid from the whole-second signing time; the
			// presigner may date it up to a second later.
			signed := tc.now.Truncate(time.Second)
			keysOutliveDeadline := !tc.creds.CanExpire || !tc.creds.Expires.Before(tc.deadline.Add(time.Second))
			if !tc.deadline.IsZero() && keysOutliveDeadline && tc.deadline.Sub(signed) <= maxPresignExpiry && signed.Add(got).Before(tc.deadline) {
				t.Errorf("URL ends at %s, before the deadline %s", signed.Add(got), tc.deadline)
			}
			if tc.creds.CanExpire && signed.Add(time.Second+got).After(tc.creds.Expires) {
				t.Errorf("URL may end at %s, after the credentials expire at %s", signed.Add(time.Second+got), tc.creds.Expires)
			}
		})
	}
}

func TestTransform_ImageEdit(t *testing.T) {
	store := newFakeS3(t, nil)
	p := newTestPlugin(t, store.URL, 0)
	dataURL := "data:image/png;base64,iVBORw0KGgo="
	payload := fmt.Appendf(nil, `{
		"model": "Qwen/Qwen-Image-Edit",
		"prompt": "make it night",
		"n": 1,
		"images": [{"image_url": "s3://audio/edits/in.png"}, {"image_url": %q}]
	}`, dataURL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	body, contentType, handled, err := p.TransformContext(ctx, payload, nil, editsURL)
	if err != nil || !handled {
		t.Fatalf("handled %v err %v, want true/nil", handled, err)
	}
	parts := parseParts(t, contentType, body)
	urls := parts["url"]
	if urls == nil || len(urls.values) != 2 {
		t.Fatalf("url fields = %+v, want one per image", urls)
	}
	signed, err := url.Parse(urls.values[0])
	if err != nil {
		t.Fatal(err)
	}
	q := signed.Query()
	if got, want := signed.Scheme+"://"+signed.Host, store.URL; got != want {
		t.Errorf("presigned host = %s, want the configured endpoint %s", got, want)
	}
	if signed.Path != "/audio/edits/in.png" {
		t.Errorf("presigned path = %q, want path-style /audio/edits/in.png", signed.Path)
	}
	// 5 minutes counted from the whole-second signing time: 300 or 301.
	if got := q.Get("X-Amz-Expires"); got != "300" && got != "301" {
		t.Errorf("X-Amz-Expires = %q, want 300-301 (the attempt's remaining time)", got)
	}
	if !strings.HasPrefix(q.Get("X-Amz-Credential"), "test-access-key/") || q.Get("X-Amz-Signature") == "" {
		t.Errorf("presigned URL is not SigV4-signed with the env credentials: %s", signed.RawQuery)
	}
	if urls.values[1] != dataURL {
		t.Errorf("second url = %q, want the data URL passed through", urls.values[1])
	}
	for name, want := range map[string]string{"model": "Qwen/Qwen-Image-Edit", "prompt": "make it night", "n": "1"} {
		if got := parts[name]; got == nil || got.values[0] != want {
			t.Errorf("field %q = %+v, want %q", name, got, want)
		}
	}
	if parts[imagesField] != nil || parts[imagesField+"[]"] != nil {
		t.Error("images must not be sent as a form field")
	}
	if n := store.requests.Load(); n != 0 {
		t.Errorf("storage requests = %d, want 0: presigning is local and vllm-omni fetches the image", n)
	}
}

func TestTransform_ImageEditRejectsBadInput(t *testing.T) {
	store := newFakeS3(t, nil)
	p := newTestPlugin(t, store.URL, 0)
	tests := map[string]string{
		"not json": `not json`,
		// Present but not a usable list: claimed, and rejected.
		"images null":        `{"images":null}`,
		"images not a list":  `{"images":"s3://audio/a.png"}`,
		"empty images":       `{"images":[]}`,
		"file_id instead":    `{"images":[{"file_id":"file-abc"}]}`,
		"https url":          `{"images":[{"image_url":"https://example.com/a.png"}]}`,
		"internal http url":  `{"images":[{"image_url":"http://169.254.169.254/latest"}]}`,
		"non-image data url": `{"images":[{"image_url":"data:text/plain;base64,QUJD"}]}`,
		"bucket not allowed": `{"images":[{"image_url":"s3://other/a.png"}]}`,
		"one bad of two":     `{"images":[{"image_url":"s3://audio/a.png"},{"image_url":"s3://other/b.png"}]}`,
		"mask given":         `{"images":[{"image_url":"s3://audio/a.png"}],"mask":{"image_url":"s3://audio/m.png"}}`,
		// A caller-supplied url would reach vllm-omni's unchecked fetch.
		"url field smuggled":       `{"images":[{"image_url":"s3://audio/a.png"}],"url":"http://169.254.169.254/latest/meta-data/"}`,
		"url[] field smuggled":     `{"images":[{"image_url":"s3://audio/a.png"}],"url[]":["http://169.254.169.254/"]}`,
		"mask_image smuggled":      `{"images":[{"image_url":"s3://audio/a.png"}],"mask_image":"http://169.254.169.254/"}`,
		"reference_image smuggled": `{"images":[{"image_url":"s3://audio/a.png"}],"reference_image":"http://169.254.169.254/"}`,
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			_, _, handled, err := p.TransformContext(context.Background(), []byte(payload), nil, editsURL)
			if handled {
				t.Error("handled = true, want false on rejection")
			}
			assertCategory(t, err, asyncapi.ErrCategoryInvalidReq)
		})
	}
}

// stsProvider stands in for an expiring-credentials source (STS, workload
// identity): call n returns keys "sts-key-n" living lifetimes[n], repeating
// the last lifetime.
type stsProvider struct {
	lifetimes []time.Duration
	calls     atomic.Int32
}

func (s *stsProvider) Retrieve(context.Context) (aws.Credentials, error) {
	n := int(s.calls.Add(1)) - 1
	return aws.Credentials{
		AccessKeyID: fmt.Sprintf("sts-key-%d", n), SecretAccessKey: "s", SessionToken: "t",
		CanExpire: true, Expires: time.Now().Add(s.lifetimes[min(n, len(s.lifetimes)-1)]),
	}, nil
}

// TestTransform_ImageEditPresignWithExpiringCredentials drives presigning
// through a real aws.CredentialsCache, as config.LoadDefaultConfig sets up.
func TestTransform_ImageEditPresignWithExpiringCredentials(t *testing.T) {
	store := newFakeS3(t, nil)
	payload := []byte(`{"prompt":"x","images":[{"image_url":"s3://audio/a.png"}]}`)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	presign := func(t *testing.T, sts *stsProvider) (url.Values, error) {
		t.Helper()
		p := newTestPlugin(t, store.URL, 0, func(o *s3.Options) { o.Credentials = aws.NewCredentialsCache(sts) })
		body, contentType, _, err := p.TransformContext(ctx, payload, nil, editsURL)
		if err != nil {
			return nil, err
		}
		signed, err := url.Parse(parseParts(t, contentType, body)["url"].values[0])
		if err != nil {
			t.Fatal(err)
		}
		return signed.Query(), nil
	}
	expires := func(t *testing.T, q url.Values) int {
		t.Helper()
		n, err := strconv.Atoi(q.Get("X-Amz-Expires"))
		if err != nil {
			t.Fatalf("X-Amz-Expires = %q", q.Get("X-Amz-Expires"))
		}
		return n
	}

	// Keys near their end are used as they are rather than dropped for a
	// refresh that might fail: the URL ends with them, a second before.
	t.Run("near-expiry keys sign a URL that ends with them", func(t *testing.T) {
		sts := &stsProvider{lifetimes: []time.Duration{30 * time.Second, time.Hour}}
		q, err := presign(t, sts)
		if err != nil {
			t.Fatal(err)
		}
		if got := expires(t, q); got < 27 || got > 29 {
			t.Errorf("X-Amz-Expires = %d, want just under the keys' 30s lifetime", got)
		}
		if !strings.HasPrefix(q.Get("X-Amz-Credential"), "sts-key-0/") || sts.calls.Load() != 1 {
			t.Errorf("signed by %q after %d retrievals, want the cached sts-key-0 after 1", q.Get("X-Amz-Credential"), sts.calls.Load())
		}
	})
	t.Run("keys shorter-lived than the attempt bound the URL", func(t *testing.T) {
		sts := &stsProvider{lifetimes: []time.Duration{2 * time.Minute}}
		q, err := presign(t, sts)
		if err != nil {
			t.Fatalf("presign failed for 2-minute keys: %v", err)
		}
		if got := expires(t, q); got < 117 || got > 119 {
			t.Errorf("X-Amz-Expires = %d, want just under the keys' 120s lifetime", got)
		}
	})
	t.Run("keys about to expire are retried", func(t *testing.T) {
		_, err := presign(t, &stsProvider{lifetimes: []time.Duration{200 * time.Millisecond}})
		assertCategory(t, err, asyncapi.ErrCategoryServer)
	})
	// A provider may hand out different keys on every call. The URL must be
	// signed by the keys whose expiry bounded it, not by whatever a second
	// retrieval returns (here: keys that die after 30s).
	t.Run("signs with the keys that bounded the URL", func(t *testing.T) {
		sts := &stsProvider{lifetimes: []time.Duration{time.Hour, 30 * time.Second}}
		p := newTestPlugin(t, store.URL, 0, func(o *s3.Options) { o.Credentials = sts })
		body, contentType, _, err := p.TransformContext(ctx, payload, nil, editsURL)
		if err != nil {
			t.Fatal(err)
		}
		signed, _ := url.Parse(parseParts(t, contentType, body)["url"].values[0])
		if got := signed.Query().Get("X-Amz-Credential"); !strings.HasPrefix(got, "sts-key-0/") {
			t.Errorf("signed by %q, want sts-key-0, the keys whose 1h lifetime bounded the URL", got)
		}
	})
	// A failed lookup fails the request, even when the cause is a network
	// error the SDK would call retryable.
	t.Run("failed credential lookup fails the request", func(t *testing.T) {
		failing := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{}, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
		})
		p := newTestPlugin(t, store.URL, 0, func(o *s3.Options) { o.Credentials = aws.NewCredentialsCache(failing) })
		_, _, _, err := p.TransformContext(ctx, payload, nil, editsURL)
		assertCategory(t, err, asyncapi.ErrCategoryAuth)
	})
}

// TestTransform_ImageEditAfterContextDone checks that a request whose context
// is already done gets the bare context error, which the worker classifies
// itself, rather than a presigned URL or a storage error category.
func TestTransform_ImageEditAfterContextDone(t *testing.T) {
	store := newFakeS3(t, nil)
	p := newTestPlugin(t, store.URL, 0)
	// Warm the credentials cache as in steady state: the cache skips its own
	// context check on a hit.
	if _, err := p.client.Options().Credentials.Retrieve(context.Background()); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()

	for name, tc := range map[string]struct {
		ctx  context.Context
		want error
	}{
		"cancelled":       {cancelled, context.Canceled},
		"deadline passed": {expired, context.DeadlineExceeded},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, handled, err := p.TransformContext(tc.ctx, []byte(`{"images":[{"image_url":"s3://audio/a.png"}]}`), nil, editsURL)
			if handled || !errors.Is(err, tc.want) {
				t.Fatalf("handled %v err %v, want false and %v", handled, err, tc.want)
			}
			var cerr *asyncapi.ClientError
			if errors.As(err, &cerr) {
				t.Errorf("err carries category %s, want a bare context error for the worker to classify", cerr.ErrorCategory)
			}
		})
	}
}
