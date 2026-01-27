//go:build cgo

package wiredtiger

/*
#cgo CFLAGS: -Wno-incompatible-pointer-types-discards-qualifiers
#cgo darwin CFLAGS: -I/usr/local/include
#cgo darwin LDFLAGS: -L/usr/local/lib -Wl,-rpath,/usr/local/lib -lwiredtiger
#cgo linux CFLAGS: -I/usr/local/include
#cgo linux LDFLAGS: -L/usr/local/lib -Wl,-rpath,/usr/local/lib -Wl,-rpath,/usr/lib/x86_64-linux-gnu -lwiredtiger
#include <stdlib.h>
#include <string.h>
#include <wiredtiger.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>

// ============================================================================
// CONNECTION OPERATIONS
// ============================================================================

static int wt_open_wrap(const char* home, const char* config, WT_CONNECTION **conn_out) {
	if (!home || !config || !conn_out) return -1;
	return wiredtiger_open(home, NULL, config, conn_out);
}

static int wt_close_wrap(WT_CONNECTION *conn) {
	if (!conn) return -1;
	return conn->close(conn, NULL);
}

static int wt_create_wrap(WT_CONNECTION *conn, const char* name, const char* config) {
	if (!conn || !name || !config) return -1;
	WT_SESSION *session = NULL;
	int err = conn->open_session(conn, NULL, NULL, &session);
	if (err != 0) {
		printf("[wt_create_wrap] Failed to open session, error: %d (%s)\n", err, wiredtiger_strerror(err));
		return err;
	}
	if (!session) return -1;
	err = session->create(session, name, config);
	if (err != 0) {
		printf("[wt_create_wrap] Failed to create table '%s', error: %d (%s)\n", name, err, wiredtiger_strerror(err));
	}
	int cerr = session->close(session, NULL);
	return err != 0 ? err : cerr;
}

static int wt_drop_wrap(WT_CONNECTION *conn, const char* name) {
	if (!conn || !name) return -1;
	WT_SESSION *session = NULL;
	int err = conn->open_session(conn, NULL, NULL, &session);
	if (err != 0) {
		printf("[wt_drop_wrap] Failed to open session, error: %d (%s)\n", err, wiredtiger_strerror(err));
		return err;
	}
	if (!session) return -1;

	// Use force=true to return success if object doesn't exist
	err = session->drop(session, name, "force=true");

	// If EBUSY, retry a few times with brief delays
	int retries = 3;
	while (err == EBUSY && retries > 0) {
		usleep(10000); // 10ms delay
		err = session->drop(session, name, "force=true");
		retries--;
	}

	if (err != 0 && err != WT_NOTFOUND) {
		printf("[wt_drop_wrap] Failed to drop table '%s', error: %d (%s)\n", name, err, wiredtiger_strerror(err));
	}

	int cerr = session->close(session, NULL);
	return err != 0 ? err : cerr;
}

int wt_table_exists(WT_CONNECTION *conn, const char* uri, int *exists) {
	if (!conn || !uri || !exists) return -1;
	*exists = 0;
	WT_SESSION *session = NULL;
	WT_CURSOR *cursor = NULL;
	int err = conn->open_session(conn, NULL, NULL, &session);
	if (err != 0) return err;
	if (!session) return -1;

	err = session->open_cursor(session, uri, NULL, NULL, &cursor);
	if (err == 0) {
		*exists = 1;
		cursor->close(cursor);
	} else if (err == 2) {
		*exists = 0;
		err = 0;
	}

	session->close(session, NULL);
	// Print error details for debugging
	if (err != 0) {
		printf("[wt_table_exists] Error opening cursor for '%s': %d (%s)\n", uri, err, wiredtiger_strerror(err));
	}
	return err;
}

// ============================================================================
// STRING KEY/VALUE OPERATIONS
// ============================================================================

static int wt_put_str(WT_CONNECTION *conn, const char* uri, const char* key, const char* val) {
	if (!conn || !uri || !key || !val) return -1;
    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    int err = conn->open_session(conn, NULL, NULL, &session);
    if (err != 0) return err;
    if (!session) return -1;
    err = session->open_cursor(session, uri, NULL, NULL, &cursor);
    if (err != 0) { session->close(session, NULL); return err; }
    if (!cursor) { session->close(session, NULL); return -1; }
    cursor->set_key(cursor, key);
    cursor->set_value(cursor, val);
    err = cursor->insert(cursor);
    int cerr = cursor->close(cursor);
    int serr = session->close(session, NULL);
    if (err != 0) return err;
    if (cerr != 0) return cerr;
    return serr;
}

static int wt_get_str(WT_CONNECTION *conn, const char* uri, const char* key, const char **outVal) {
	if (!conn || !uri || !key || !outVal) return -1;
    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    int err = conn->open_session(conn, NULL, NULL, &session);
    if (err != 0) return err;
    if (!session) return -1;
    err = session->open_cursor(session, uri, NULL, NULL, &cursor);
    if (err != 0) { session->close(session, NULL); return err; }
    if (!cursor) { session->close(session, NULL); return -1; }
    cursor->set_key(cursor, key);
    err = cursor->search(cursor);
    if (err != 0) { cursor->close(cursor); session->close(session, NULL); return err; }
    const char *val;
    err = cursor->get_value(cursor, &val);
    if (err == 0) { *outVal = val; }
    int cerr = cursor->close(cursor);
    int serr = session->close(session, NULL);
    if (err != 0) return err;
    if (cerr != 0) return cerr;
    return serr;
}

static int wt_del_str(WT_CONNECTION *conn, const char* uri, const char* key) {
	if (!conn || !uri || !key) return -1;
    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    int err = conn->open_session(conn, NULL, NULL, &session);
    if (err != 0) return err;
    if (!session) return -1;
    err = session->open_cursor(session, uri, NULL, NULL, &cursor);
    if (err != 0) { session->close(session, NULL); return err; }
    if (!cursor) { session->close(session, NULL); return -1; }
    cursor->set_key(cursor, key);
    err = cursor->remove(cursor);
    int cerr = cursor->close(cursor);
    int serr = session->close(session, NULL);
    if (err != 0) return err;
    if (cerr != 0) return cerr;
    return serr;
}

static int wt_exists_str(WT_CONNECTION *conn, const char* uri, const char* key, int *found) {
	if (!conn || !uri || !key || !found) return -1;
    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    *found = 0;
    int err = conn->open_session(conn, NULL, NULL, &session);
    if (err != 0) return err;
    if (!session) return -1;
    err = session->open_cursor(session, uri, NULL, NULL, &cursor);
    if (err != 0) { session->close(session, NULL); return err; }
    if (!cursor) { session->close(session, NULL); return -1; }
    cursor->set_key(cursor, key);
    err = cursor->search(cursor);
    if (err == 0) *found = 1;
    int cerr = cursor->close(cursor);
    int serr = session->close(session, NULL);
    if (err != 0 && err != WT_NOTFOUND) return err;
    if (cerr != 0) return cerr;
    return serr;
}

// ============================================================================
// BINARY KEY/VALUE OPERATIONS
// ============================================================================

static int wt_put_bin(WT_CONNECTION *conn, const char* uri,
                      const unsigned char* key, size_t key_len,
                      const unsigned char* val, size_t val_len) {
	if (!conn || !uri || !key || !val) return -1;
    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    int err = conn->open_session(conn, NULL, NULL, &session);
    if (err != 0) return err;
    if (!session) return -1;

    err = session->open_cursor(session, uri, NULL, NULL, &cursor);
    if (err != 0) { session->close(session, NULL); return err; }
    if (!cursor) { session->close(session, NULL); return -1; }

    WT_ITEM key_item;
    key_item.data = (void*)key;
    key_item.size = key_len;
    cursor->set_key(cursor, &key_item);

    WT_ITEM val_item;
    val_item.data = (void*)val;
    val_item.size = val_len;
    cursor->set_value(cursor, &val_item);

    err = cursor->insert(cursor);
    int cerr = cursor->close(cursor);
    int serr = session->close(session, NULL);
    if (err != 0) return err;
    if (cerr != 0) return cerr;
    return serr;
}

static int wt_get_bin(WT_CONNECTION *conn, const char* uri,
                      const unsigned char* key, size_t key_len,
                      WT_ITEM *outVal) {
	if (!conn || !uri || !key || !outVal) return -1;

	// Initialize output to safe values
	outVal->data = NULL;
	outVal->size = 0;

	if (key_len == 0 || key_len > 1024) {
		return -1; // Invalid key length
	}

    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    int err = conn->open_session(conn, NULL, NULL, &session);
    if (err != 0) return err;
    if (!session) return -1;

    err = session->open_cursor(session, uri, NULL, NULL, &cursor);
    if (err != 0) {
    	session->close(session, NULL);
    	return err;
    }
    if (!cursor) {
    	session->close(session, NULL);
    	return -1;
    }

    WT_ITEM key_item;
    key_item.data = (void*)key;
    key_item.size = key_len;
    cursor->set_key(cursor, &key_item);

    err = cursor->search(cursor);
    if (err != 0) {
    	cursor->close(cursor);
    	session->close(session, NULL);
    	return err;
    }

    WT_ITEM val;
    err = cursor->get_value(cursor, &val);
    if (err == 0 && val.data && val.size > 0) {
        outVal->data = malloc(val.size);
        if (!outVal->data) {
            cursor->close(cursor);
            session->close(session, NULL);
            return -1;
        }
        memcpy(outVal->data, val.data, val.size);
        outVal->size = val.size;
    } else {
    	// No data found or invalid data
    	outVal->data = NULL;
    	outVal->size = 0;
    }

    int cerr = cursor->close(cursor);
    int serr = session->close(session, NULL);
    if (err != 0) return err;
    if (cerr != 0) return cerr;
    return serr;
}

static int wt_del_bin(WT_CONNECTION *conn, const char* uri,
                      const unsigned char* key, size_t key_len) {
	if (!conn || !uri || !key) return -1;
    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    int err = conn->open_session(conn, NULL, NULL, &session);
    if (err != 0) return err;
    if (!session) return -1;

    err = session->open_cursor(session, uri, NULL, NULL, &cursor);
    if (err != 0) { session->close(session, NULL); return err; }
    if (!cursor) { session->close(session, NULL); return -1; }

    WT_ITEM key_item;
    key_item.data = (void*)key;
    key_item.size = key_len;
    cursor->set_key(cursor, &key_item);

    err = cursor->remove(cursor);
    int cerr = cursor->close(cursor);
    int serr = session->close(session, NULL);
    if (err != 0) return err;
    if (cerr != 0) return cerr;
    return serr;
}

static int wt_exists_bin(WT_CONNECTION *conn, const char* uri,
                         const unsigned char* key, size_t key_len,
                         int *found) {
	if (!conn || !uri || !key || !found) return -1;
    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    *found = 0;

    int err = conn->open_session(conn, NULL, NULL, &session);
    if (err != 0) return err;
    if (!session) return -1;

    err = session->open_cursor(session, uri, NULL, NULL, &cursor);
    if (err != 0) { session->close(session, NULL); return err; }
    if (!cursor) { session->close(session, NULL); return -1; }

    WT_ITEM key_item;
    key_item.data = (void*)key;
    key_item.size = key_len;
    cursor->set_key(cursor, &key_item);

    err = cursor->search(cursor);
    if (err == 0) *found = 1;

    int cerr = cursor->close(cursor);
    int serr = session->close(session, NULL);
    if (err != 0 && err != WT_NOTFOUND) return err;
    if (cerr != 0) return cerr;
    return serr;
}

// ============================================================================
// SCAN OPERATIONS
// ============================================================================

typedef struct {
    char **keys;
    char **vals;
    int len;
} wt_vec_t;

static void wt_vec_free(wt_vec_t vec) {
    if (vec.keys) {
        for (int i = 0; i < vec.len; i++) if (vec.keys[i]) free(vec.keys[i]);
        free(vec.keys);
    }
    if (vec.vals) {
        for (int i = 0; i < vec.len; i++) if (vec.vals[i]) free(vec.vals[i]);
        free(vec.vals);
    }
}

static int wt_scan_collect(WT_CONNECTION *conn, const char* uri, int max, wt_vec_t *out) {
	if (!conn || !uri || !out) return -1;
    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    int err = 0;
    memset(out, 0, sizeof(*out));
    out->keys = (char**)calloc((size_t)max, sizeof(char*));
    out->vals = (char**)calloc((size_t)max, sizeof(char*));
    if (!out->keys || !out->vals) { err = -1; goto done; }
    err = conn->open_session(conn, NULL, NULL, &session);
    if (err != 0) goto done;
    if (!session) { err = -1; goto done; }
    err = session->open_cursor(session, uri, NULL, NULL, &cursor);
    if (err != 0) goto done;
    if (!cursor) { err = -1; goto done; }
    const char *k; const char *v;
    while ((err = cursor->next(cursor)) == 0) {
        if (cursor->get_key(cursor, &k) != 0) { err = -1; break; }
        if (cursor->get_value(cursor, &v) != 0) { err = -1; break; }
        if (out->len >= max) break;
        out->keys[out->len] = strdup(k);
        out->vals[out->len] = strdup(v);
        if (!out->keys[out->len] || !out->vals[out->len]) { err = -1; break; }
        out->len++;
    }
    if (err == WT_NOTFOUND) err = 0;
done:
    if (cursor) cursor->close(cursor);
    if (session) session->close(session, NULL);
    return err;
}

typedef struct {
    unsigned char **keys;
    size_t *key_lens;
    unsigned char **vals;
    size_t *val_lens;
    int len;
} wt_bin_vec_t;

static void wt_bin_vec_free(wt_bin_vec_t vec) {
    if (vec.keys) {
        for (int i = 0; i < vec.len; i++) if (vec.keys[i]) free(vec.keys[i]);
        free(vec.keys);
    }
    if (vec.vals) {
        for (int i = 0; i < vec.len; i++) if (vec.vals[i]) free(vec.vals[i]);
        free(vec.vals);
    }
    if (vec.key_lens) free(vec.key_lens);
    if (vec.val_lens) free(vec.val_lens);
}

static int wt_scan_bin_collect(WT_CONNECTION *conn, const char* uri, int max, wt_bin_vec_t *out) {
	if (!conn || !uri || !out) return -1;
    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    int err = 0;
    memset(out, 0, sizeof(*out));

    out->keys = (unsigned char**)calloc((size_t)max, sizeof(unsigned char*));
    out->vals = (unsigned char**)calloc((size_t)max, sizeof(unsigned char*));
    out->key_lens = (size_t*)calloc((size_t)max, sizeof(size_t));
    out->val_lens = (size_t*)calloc((size_t)max, sizeof(size_t));

    if (!out->keys || !out->vals || !out->key_lens || !out->val_lens) {
        err = -1;
        goto done;
    }

    err = conn->open_session(conn, NULL, NULL, &session);
    if (err != 0) goto done;
    if (!session) { err = -1; goto done; }

    err = session->open_cursor(session, uri, NULL, NULL, &cursor);
    if (err != 0) goto done;
    if (!cursor) { err = -1; goto done; }

    WT_ITEM k; WT_ITEM v;
    while ((err = cursor->next(cursor)) == 0) {
        if (cursor->get_key(cursor, &k) != 0) { err = -1; break; }
        if (cursor->get_value(cursor, &v) != 0) { err = -1; break; }
        if (out->len >= max) break;

        out->keys[out->len] = (unsigned char*)malloc(k.size);
        if (!out->keys[out->len]) { err = -1; break; }
        memcpy(out->keys[out->len], k.data, k.size);
        out->key_lens[out->len] = k.size;

        out->vals[out->len] = (unsigned char*)malloc(v.size);
        if (!out->vals[out->len]) { err = -1; break; }
        memcpy(out->vals[out->len], v.data, v.size);
        out->val_lens[out->len] = v.size;

        out->len++;
    }
    if (err == WT_NOTFOUND) err = 0;

done:
    if (cursor) cursor->close(cursor);
    if (session) session->close(session, NULL);
    return err;
}

static int wt_search_near_str(WT_CONNECTION *conn, const char* uri, const char* key,
                              const char **outKey, const char **outVal, int *exact) {
	if (!conn || !uri || !key || !outKey || !outVal || !exact) return -1;
    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    int err = conn->open_session(conn, NULL, NULL, &session);
    if (err != 0) return err;
    if (!session) return -1;
    err = session->open_cursor(session, uri, NULL, NULL, &cursor);
    if (err != 0) { session->close(session, NULL); return err; }
    if (!cursor) { session->close(session, NULL); return -1; }
    cursor->set_key(cursor, key);
    err = cursor->search_near(cursor, exact);
    if (err == 0) {
        const char *k; const char *v;
        if (cursor->get_key(cursor, &k) == 0 && cursor->get_value(cursor, &v) == 0) {
            *outKey = k; *outVal = v;
        }
    }
    int cerr = cursor->close(cursor);
    int serr = session->close(session, NULL);
    if (cerr != 0) return cerr;
    return err != 0 ? err : serr;
}

static int wt_search_near_bin(WT_CONNECTION *conn, const char* uri,
                              const unsigned char* key, size_t key_len,
                              WT_ITEM *outKey, WT_ITEM *outVal, int *exact) {
	if (!conn || !uri || !key || !outKey || !outVal || !exact) return -1;
    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    int err = conn->open_session(conn, NULL, NULL, &session);
    if (err != 0) return err;
    if (!session) return -1;

    err = session->open_cursor(session, uri, NULL, NULL, &cursor);
    if (err != 0) { session->close(session, NULL); return err; }
    if (!cursor) { session->close(session, NULL); return -1; }

    WT_ITEM key_item;
    key_item.data = (void*)key;
    key_item.size = key_len;
    cursor->set_key(cursor, &key_item);

    err = cursor->search_near(cursor, exact);
    if (err == 0) {
        WT_ITEM k; WT_ITEM v;
        if (cursor->get_key(cursor, &k) == 0 && cursor->get_value(cursor, &v) == 0) {
            outKey->data = malloc(k.size);
            if (outKey->data) {
                memcpy(outKey->data, k.data, k.size);
                outKey->size = k.size;
            }
            outVal->data = malloc(v.size);
            if (outVal->data) {
                memcpy(outVal->data, v.data, v.size);
                outVal->size = v.size;
            }
        }
    }

    int cerr = cursor->close(cursor);
    int serr = session->close(session, NULL);
    if (cerr != 0) return cerr;
    return err != 0 ? err : serr;
}

// ============================================================================
// RANGE SCAN OPERATIONS (string keys)
// ============================================================================

typedef struct {
    WT_SESSION *session;
    WT_CURSOR *cursor;
    int   err;
    int   valid;     // 1 if positioned at valid entry
    int   in_range; // 1 if still in user range
    char *end_key;   // malloc'd, for bounds
} wt_range_ctx_t;

static wt_range_ctx_t* wt_range_scan_init_str(WT_CONNECTION *conn, const char* uri, const char* start_key, const char* end_key) {
    wt_range_ctx_t *ctx = malloc(sizeof(wt_range_ctx_t));
    if (!ctx) return NULL;
    memset(ctx, 0, sizeof(*ctx));
    ctx->err = 0; ctx->valid = 0; ctx->in_range = 1;
    if (!conn || !uri || !start_key || !end_key) { free(ctx); return NULL; }

    // Validate that keys are not empty to prevent WiredTiger panic
    if (strlen(start_key) == 0 || strlen(end_key) == 0) {
        free(ctx);
        return NULL;
    }

    ctx->session = NULL;
    ctx->cursor = NULL;
    ctx->end_key = strdup(end_key);

    // Open session
    int err = conn->open_session(conn, NULL, NULL, &ctx->session);
    if (err != 0 || !ctx->session) {
        printf("[wt_range_scan_init_str] Failed to open session, error: %d\n", err);
        free(ctx->end_key); free(ctx); return NULL;
    }
    printf("[wt_range_scan_init_str] Session opened successfully\n");

    // Open cursor
    err = ctx->session->open_cursor(ctx->session, uri, NULL, NULL, &ctx->cursor);
    if (err != 0 || !ctx->cursor) {
        printf("[wt_range_scan_init_str] Failed to open cursor, error: %d\n", err);
        ctx->session->close(ctx->session, NULL);
        free(ctx->end_key); free(ctx);
        return NULL;
    }

    printf("[wt_range_scan_init_str] Cursor opened successfully\n");
    printf("[wt_range_scan_init_str] start_key: %s, end_key: %s\n", start_key, end_key);

    // Position at start_key (with search_near pattern)
    ctx->cursor->set_key(ctx->cursor, start_key);

    int exact = 0;
    err = ctx->cursor->search_near(ctx->cursor, &exact);
    switch (exact) {
    case -1: {
        // search_near landed before start_key; advance to first key > start_key
        int err_reset = ctx->cursor->reset(ctx->cursor);
        if (err_reset != 0) {
            ctx->cursor->close(ctx->cursor);
            ctx->session->close(ctx->session, NULL);
            free(ctx->end_key); free(ctx);
            return NULL;
        }
        // After reset, must call next() and check bounds
        int err_next = ctx->cursor->next(ctx->cursor);
        if (err_next != 0) {
            ctx->in_range = 0;
            ctx->valid = 0;
            // Still return ctx so caller sees empty range, not NULL/fatal
            return ctx;
        }
        // Check bounds: key < end_key?
        const char *curr = NULL;
        if (ctx->cursor->get_key(ctx->cursor, &curr) == 0) {
            if (strcmp(curr, ctx->end_key) >= 0) {
                ctx->in_range = 0;
                ctx->valid = 0;
            } else {
                ctx->in_range = 1;
                ctx->valid = 1;
            }
        } else {
            ctx->in_range = 0;
            ctx->valid = 0;
        }
        return ctx;
    }

    case 0:
    case 1: {

        // Now cursor is positioned at:
        // - exact match of start_key, OR
        // - first key > start_key (if no exact match)
        // Check bounds: current < end_key?
        const char *curr;
        if (ctx->cursor->get_key(ctx->cursor, &curr) == 0) {
            if (strcmp(curr, ctx->end_key) >= 0) {
                ctx->in_range = 0;
                ctx->valid = 0;
            } else {
                ctx->in_range = 1;
                ctx->valid = 1;
            }
        } else {
            // Set BOOLS to false
            ctx->in_range = 0;
            ctx->valid = 0;
        }
        return ctx;
    }
    }
    ctx->cursor->close(ctx->cursor);
    ctx->session->close(ctx->session, NULL);
    free(ctx->end_key);
    free(ctx);
    return NULL;
}


static int wt_range_scan_next(wt_range_ctx_t* ctx, const char **out_key, const char **out_val, int* in_range) {
    if (!ctx) return -1;
    int err = ctx->cursor->next(ctx->cursor);
    if (err != 0) { ctx->valid = 0; ctx->in_range = 0; *in_range = 0; return err; }
    const char *key = NULL; const char* val = NULL;
    ctx->cursor->get_key(ctx->cursor, &key);
    ctx->cursor->get_value(ctx->cursor, &val);
    // Check bounds: key < end_key
    if (strcmp(key, ctx->end_key) >= 0) {
        ctx->in_range = 0;
        ctx->valid = 0;
        *in_range = 0;
        return 1; // not an error, just OOB
    }
    ctx->in_range = 1; ctx->valid = 1;
    *out_key = key;
    *out_val = val;
    *in_range = 1;
    return 0;
}

// Retrieve current key/val (no iteration/movement)
static int wt_range_scan_current(wt_range_ctx_t* ctx, const char **out_key, const char **out_val) {
    if (!ctx  || !ctx->valid) return -1;
    return ctx->cursor->get_key(ctx->cursor, out_key) == 0 && ctx->cursor->get_value(ctx->cursor, out_val) == 0 ? 0 : -1;
}

// Close/finalize & free ctx and internal resources
static void wt_range_scan_close(wt_range_ctx_t* ctx) {
    if (!ctx) return;
    if (ctx->cursor) ctx->cursor->close(ctx->cursor);
    if (ctx->session) ctx->session->close(ctx->session, NULL);
    if (ctx->end_key) free(ctx->end_key);
    free(ctx);
}

// ============================================================================
// BATCH RANGE SCAN OPERATIONS (string keys)
// ============================================================================

// Batch buffer structure for efficient data transfer
typedef struct {
    char *data;        // Packed key-value data
    size_t capacity;  // Total allocated capacity
    size_t length;    // Current data length
    size_t count;     // Number of records in buffer
} wt_batch_buf_t;

// Initialize batch buffer with initial capacity
static wt_batch_buf_t* wt_batch_buf_init(size_t initial_capacity) {
    wt_batch_buf_t *buf = malloc(sizeof(wt_batch_buf_t));
    if (!buf) return NULL;

    buf->data = malloc(initial_capacity);
    if (!buf->data) {
        free(buf);
        return NULL;
    }

    buf->capacity = initial_capacity;
    buf->length = 0;
    buf->count = 0;
    return buf;
}

// Ensure buffer has enough capacity for additional data
static int wt_batch_buf_ensure_capacity(wt_batch_buf_t *buf, size_t needed) {
    if (!buf) return -1;

    if (buf->length + needed <= buf->capacity) {
        return 0; // Already has enough capacity
    }

    // Double capacity strategy for amortized O(1) growth
    size_t new_capacity = buf->capacity * 2;
    while (new_capacity < buf->length + needed) {
        new_capacity *= 2;
    }

    char *new_data = realloc(buf->data, new_capacity);
    if (!new_data) return -1;

    buf->data = new_data;
    buf->capacity = new_capacity;
    return 0;
}

// Append a key-value pair to the batch buffer
// Format: [key_len (4 bytes)][key_data][val_len (4 bytes)][val_data]
static int wt_batch_buf_append_kv(wt_batch_buf_t *buf, const char *key, const char *val) {
    if (!buf || !key || !val) return -1;

    size_t key_len = strlen(key);
    size_t val_len = strlen(val);
    size_t total_needed = 4 + key_len + 4 + val_len; // lengths + data

    if (wt_batch_buf_ensure_capacity(buf, total_needed) != 0) {
        return -1;
    }

    char *ptr = buf->data + buf->length;

    // Write key length (little-endian uint32)
    uint32_t key_len_le = (uint32_t)key_len;
    memcpy(ptr, &key_len_le, 4);
    ptr += 4;

    // Write key data
    memcpy(ptr, key, key_len);
    ptr += key_len;

    // Write value length (little-endian uint32)
    uint32_t val_len_le = (uint32_t)val_len;
    memcpy(ptr, &val_len_le, 4);
    ptr += 4;

    // Write value data
    memcpy(ptr, val, val_len);
    ptr += val_len;

    buf->length = ptr - buf->data;
    buf->count++;
    return 0;
}

// Free batch buffer and all associated memory
static void wt_batch_buf_free(wt_batch_buf_t *buf) {
    if (!buf) return;
    if (buf->data) free(buf->data);
    free(buf);
}

// High-performance batch range scan implementation
// Fetches up to max_records key-value pairs in a single operation
static int wt_range_scan_next_batch(wt_range_ctx_t* ctx, int max_records,
                                   char **out_buf, int *out_buf_len, int *out_count) {
    if (!ctx || !out_buf || !out_buf_len || !out_count) {
        return -1;
    }

    // Initialize output parameters
    *out_buf = NULL;
    *out_buf_len = 0;
    *out_count = 0;

    // If cursor is not valid or out of range, return empty batch
    if (!ctx->valid || !ctx->in_range) {
        return 0; // Success, but empty batch
    }

    // Initialize batch buffer with reasonable initial capacity
    // Estimate: average 50 chars per key + 100 chars per value = ~150 bytes per record
    size_t estimated_size = max_records * 150;
    wt_batch_buf_t *batch_buf = wt_batch_buf_init(estimated_size);
    if (!batch_buf) {
        return -1; // Memory allocation failed
    }

    int records_fetched = 0;
    int err = 0;

    // Fetch records in batch
    for (int i = 0; i < max_records; i++) {
        const char *key = NULL;
        const char *val = NULL;

        // Get current key-value pair
        err = ctx->cursor->get_key(ctx->cursor, &key);
        if (err != 0) break;

        err = ctx->cursor->get_value(ctx->cursor, &val);
        if (err != 0) break;

        // Check bounds: key < end_key
        if (strcmp(key, ctx->end_key) >= 0) {
            ctx->in_range = 0;
            ctx->valid = 0;
            break; // Out of range, but not an error
        }

        // Append to batch buffer
        if (wt_batch_buf_append_kv(batch_buf, key, val) != 0) {
            err = -1;
            break;
        }

        records_fetched++;

        // Advance to next record
        err = ctx->cursor->next(ctx->cursor);
        if (err != 0) {
            // If we got some records before hitting an error, that's still success
            if (records_fetched > 0) {
                err = 0; // Treat as success with partial batch
            }
            break;
        }
    }

    // Set output parameters
    if (records_fetched > 0) {
        *out_buf = batch_buf->data;
        *out_buf_len = (int)batch_buf->length;
        *out_count = records_fetched;

        // Transfer ownership of data buffer to caller
        // Don't free batch_buf->data here - it will be freed by wt_free_batch_buf
        batch_buf->data = NULL; // Prevent double-free
    }

    // Always free the batch buffer struct
    wt_batch_buf_free(batch_buf);

    return err;
}

// Free a batch buffer returned by wt_range_scan_next_batch
static void wt_free_batch_buf(char *buf) {
    if (buf) {
        free(buf);
    }
}

// ============================================================================
// RANGE SCAN OPERATIONS (binary keys)
// ============================================================================

typedef struct {
    WT_SESSION *session;
    WT_CURSOR  *cursor;
    int         err;
    int         valid;      // 1 if cursor is on a valid entry
    int         in_range;   // 1 if cursor is within the scan bounds
    WT_ITEM     end_key;    // A copy of the end key for bounds checking
} wt_range_ctx_bin_t;

static void wt_range_scan_close_bin(wt_range_ctx_bin_t* ctx);

// Helper to compare two WT_ITEMs lexicographically.
static int compare_wt_items(WT_ITEM *a, WT_ITEM *b) {
    if (!a || !b) {
        return 0;
    }

    size_t min_len = a->size < b->size ? a->size : b->size;
    int cmp = memcmp(a->data, b->data, min_len);
    if (cmp != 0) {
        return cmp;
    }
    if (a->size < b->size) {
        return -1;
    }
    if (a->size > b->size) {
        return 1;
    }
    return 0;
}

// Initializes a binary range scan.
static wt_range_ctx_bin_t* wt_range_scan_init_bin(WT_CONNECTION *conn, const char* uri,
                                                  WT_ITEM *start_key, WT_ITEM *end_key) {
    if (!conn || !uri || !start_key || !end_key) {
        return NULL;
    }

    wt_range_ctx_bin_t *ctx = calloc(1, sizeof(wt_range_ctx_bin_t));
    if (!ctx) {
        return NULL;
    }

    // Copy end_key for bounds checking
    if (end_key->size > 0) {
        ctx->end_key.data = malloc(end_key->size);
        if (!ctx->end_key.data) {
            free(ctx);
            return NULL;
        }
        memcpy(ctx->end_key.data, end_key->data, end_key->size);
        ctx->end_key.size = end_key->size;
    }

    int err = conn->open_session(conn, NULL, NULL, &ctx->session);
    if (err != 0 || !ctx->session) {
        if (ctx->end_key.data) free(ctx->end_key.data);
        free(ctx);
        return NULL;
    }

    err = ctx->session->open_cursor(ctx->session, uri, NULL, NULL, &ctx->cursor);
    if (err != 0 || !ctx->cursor) {
        ctx->session->close(ctx->session, NULL);
        if (ctx->end_key.data) free(ctx->end_key.data);
        free(ctx);
        return NULL;
    }

    // Position the cursor at the start of the range.
    if (start_key->size == 0) {
        // This is a full table scan from the beginning.
        err = ctx->cursor->next(ctx->cursor);
        if (err == WT_NOTFOUND) {
            ctx->valid = 0;
            ctx->in_range = 0;
            return ctx; // Table is empty, not an error.
        } else if (err != 0) {
            wt_range_scan_close_bin(ctx);
            return NULL; // Fatal error
        }
    } else {
        // This is a range scan from a specific start key.
        ctx->cursor->set_key(ctx->cursor, start_key);
        int exact;
        err = ctx->cursor->search_near(ctx->cursor, &exact);

        if (err != 0) {
            if (err == WT_NOTFOUND) {
                ctx->valid = 0; // No keys >= start_key
                ctx->in_range = 0;
                return ctx;
            }
            wt_range_scan_close_bin(ctx);
            return NULL;
        }

        if (exact < 0) {
            // search_near landed before start_key, advance to the next record.
            err = ctx->cursor->next(ctx->cursor);
            if (err != 0) {
                if (err == WT_NOTFOUND) {
                    ctx->valid = 0; // No keys >= start_key
                    ctx->in_range = 0;
                    return ctx;
                }
                wt_range_scan_close_bin(ctx);
                return NULL;
            }
        }
    }

    // Verify current position is within [start, end)
    WT_ITEM curr_key;
    if (ctx->cursor->get_key(ctx->cursor, &curr_key) != 0) {
        ctx->valid = 0;
        ctx->in_range = 0;
        return ctx;
    }
    if (end_key->size > 0 && compare_wt_items(&curr_key, &ctx->end_key) >= 0) {
        ctx->valid = 0;
        ctx->in_range = 0;
    } else {
        ctx->valid = 1;
        ctx->in_range = 1;
    }

    return ctx;
}

// Simple one-by-one binary range scan - get current key/value
static int wt_range_scan_current_bin(wt_range_ctx_bin_t* ctx, WT_ITEM *out_key, WT_ITEM *out_val) {
    if (!ctx || !out_key || !out_val) {
        return -1;
    }

    if (!ctx->valid || !ctx->in_range) {
        return 1; // End of scan
    }

    // Initialize output items
    out_key->data = NULL;
    out_key->size = 0;
    out_val->data = NULL;
    out_val->size = 0;

    WT_ITEM key, val;
    int err = ctx->cursor->get_key(ctx->cursor, &key);
    if (err != 0) {
        ctx->valid = 0;
        return err;
    }
    err = ctx->cursor->get_value(ctx->cursor, &val);
    if (err != 0) {
        ctx->valid = 0;
        return err;
    }

    // Copy key
    out_key->data = malloc(key.size);
    if (!out_key->data) {
        return -1;
    }
    memcpy(out_key->data, key.data, key.size);
    out_key->size = key.size;

    // Copy value
    out_val->data = malloc(val.size);
    if (!out_val->data) {
        free(out_key->data);
        return -1;
    }
    memcpy(out_val->data, val.data, val.size);
    out_val->size = val.size;

    return 0;
}

// Advance to next record in binary range scan
static int wt_range_scan_next_bin(wt_range_ctx_bin_t* ctx) {
    if (!ctx) {
        return -1;
    }

    if (!ctx->valid) {
        return 1; // Already at end
    }

    int err = ctx->cursor->next(ctx->cursor);
    if (err != 0) {
        ctx->valid = 0;
        return err == WT_NOTFOUND ? 1 : err; // WT_NOTFOUND means end of scan
    }

    // Check if next key is within bounds
    WT_ITEM next_key;
    err = ctx->cursor->get_key(ctx->cursor, &next_key);
    if (err != 0) {
        ctx->valid = 0;
        return err;
    }

    if (ctx->end_key.size > 0 && compare_wt_items(&next_key, &ctx->end_key) >= 0) {
        ctx->valid = 0;
        ctx->in_range = 0;
        return 1; // Out of range
    }

    return 0;
}

// Frees the scan context and associated resources.
static void wt_range_scan_close_bin(wt_range_ctx_bin_t* ctx) {
    if (!ctx) return;
    if (ctx->cursor) ctx->cursor->close(ctx->cursor);
    if (ctx->session) ctx->session->close(ctx->session, NULL);
    if (ctx->end_key.data) free(ctx->end_key.data);
    free(ctx);
}

// Free function for binary range scan items
static void wt_free_binary_item(WT_ITEM *item) {
    if (item && item->data) {
        free(item->data);
        item->data = NULL;
        item->size = 0;
    }
}

// Fetches a batch of key-value pairs for binary scans.
// Buffer layout: [count u32][key_len u32][key bytes][val_len u32][val bytes] ...
static int wt_range_scan_next_batch_bin(wt_range_ctx_bin_t* ctx, size_t max_buf_size,
    unsigned char **out_buf, int *out_buf_len, int *out_count) {
    if (!ctx || !out_buf || !out_buf_len || !out_count) return -1;
    *out_buf = NULL; *out_buf_len = 0; *out_count = 0;
    if (!ctx->valid || !ctx->in_range) return 0;

    size_t capacity = max_buf_size > 0 ? max_buf_size : (size_t)1024 * 1024;
    unsigned char *buf = (unsigned char*)malloc(capacity);
    if (!buf) return -1;

    unsigned char *ptr = buf;
    size_t length = 0;
    int count = 0;

    // reserve space for count
    if (capacity < sizeof(uint32_t)) { free(buf); return -1; }
    ptr += sizeof(uint32_t);
    length += sizeof(uint32_t);

    while (ctx->valid && ctx->in_range) {
        WT_ITEM key, val;
        if (ctx->cursor->get_key(ctx->cursor, &key) != 0) { ctx->valid = 0; break; }
        if (ctx->cursor->get_value(ctx->cursor, &val) != 0) { ctx->valid = 0; break; }

        if (ctx->end_key.size > 0 && compare_wt_items(&key, &ctx->end_key) >= 0) {
            ctx->in_range = 0; ctx->valid = 0; break;
        }

        size_t need = sizeof(uint32_t) + key.size + sizeof(uint32_t) + val.size;
        if (length + need > capacity) break; // stop when full

        uint32_t klen = (uint32_t)key.size;
        memcpy(ptr, &klen, sizeof(klen)); ptr += sizeof(klen);
        memcpy(ptr, key.data, key.size); ptr += key.size;
        uint32_t vlen = (uint32_t)val.size;
        memcpy(ptr, &vlen, sizeof(vlen)); ptr += sizeof(vlen);
        memcpy(ptr, val.data, val.size); ptr += val.size;
        length = (size_t)(ptr - buf);
        count++;

        int nerr = ctx->cursor->next(ctx->cursor);
        if (nerr != 0) { ctx->valid = 0; break; }
        if ((size_t)count >= 1000) break; // safety cap per batch
    }

    if (count > 0) {
        uint32_t cnt = (uint32_t)count;
        memcpy(buf, &cnt, sizeof(cnt));
        *out_buf = buf;
        *out_buf_len = (int)length;
        *out_count = count;
        return 0;
    }
    free(buf);
    return 0;
}

static void wt_free_batch_buf_bin(unsigned char *buf) { if (buf) free(buf); }

// ============================================================================
// BATCH WRITER OPERATIONS
// ============================================================================

typedef struct {
    WT_SESSION *session;
    WT_CURSOR *cursor;
    int is_binary;  // 1 for binary format, 0 for string format
} wt_batch_ctx_t;

// Open a batch writer context - keeps session and cursor open for multiple writes
static wt_batch_ctx_t* wt_batch_open(WT_CONNECTION *conn, const char* uri, int is_binary) {
    if (!conn || !uri) return NULL;

    wt_batch_ctx_t *ctx = (wt_batch_ctx_t*)calloc(1, sizeof(wt_batch_ctx_t));
    if (!ctx) return NULL;

    ctx->is_binary = is_binary;

    int err = conn->open_session(conn, NULL, NULL, &ctx->session);
    if (err != 0 || !ctx->session) {
        free(ctx);
        return NULL;
    }

    err = ctx->session->open_cursor(ctx->session, uri, NULL, NULL, &ctx->cursor);
    if (err != 0 || !ctx->cursor) {
        ctx->session->close(ctx->session, NULL);
        free(ctx);
        return NULL;
    }

    return ctx;
}

// Write a binary key-value pair using batch context
static int wt_batch_put_bin(wt_batch_ctx_t *ctx,
                            const unsigned char* key, size_t key_len,
                            const unsigned char* val, size_t val_len) {
    if (!ctx || !ctx->cursor || !key || !val) return -1;

    WT_ITEM key_item;
    key_item.data = (void*)key;
    key_item.size = key_len;
    ctx->cursor->set_key(ctx->cursor, &key_item);

    WT_ITEM val_item;
    val_item.data = (void*)val;
    val_item.size = val_len;
    ctx->cursor->set_value(ctx->cursor, &val_item);

    return ctx->cursor->insert(ctx->cursor);
}

// Write a string key-value pair using batch context
static int wt_batch_put_str(wt_batch_ctx_t *ctx, const char* key, const char* val) {
    if (!ctx || !ctx->cursor || !key || !val) return -1;

    ctx->cursor->set_key(ctx->cursor, key);
    ctx->cursor->set_value(ctx->cursor, val);

    return ctx->cursor->insert(ctx->cursor);
}

// Close the batch context and release resources
static int wt_batch_close(wt_batch_ctx_t *ctx) {
    if (!ctx) return 0;

    int err = 0;
    if (ctx->cursor) {
        err = ctx->cursor->close(ctx->cursor);
    }
    if (ctx->session) {
        int serr = ctx->session->close(ctx->session, NULL);
        if (err == 0) err = serr;
    }
    free(ctx);
    return err;
}

*/
import "C"
import (
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"
)

type cgoService struct {
	conn *C.WT_CONNECTION
}

func WiredTigerService() WTService { return &cgoService{} }

// Error codes for familiar WiredTiger errors
var (
	ErrNotFound      = errors.New("wiredtiger: not found")
	ErrBusy          = errors.New("wiredtiger: resource busy (EBUSY)")
	ErrRollback      = errors.New("wiredtiger: rollback conflict")
	ErrDuplicateKey  = errors.New("wiredtiger: duplicate key")
	ErrPanic         = errors.New("wiredtiger: panic")
	ErrWriteConflict = errors.New("wiredtiger: write conflict")
)

// mapWiretigerError maps integer error codes from WiredTiger to standard Go errors where possible.
func mapWiretigerError(code C.int) error {
	switch code {
	case 0:
		return nil
	case -31803: // WT_NOTFOUND
		return ErrNotFound
	case -31804: // WT_DUPLICATE_KEY
		return ErrDuplicateKey
	case -31805: // WT_ROLLBACK
		return ErrRollback
	case C.int(C.EBUSY):
		return ErrBusy
	}
	return fmt.Errorf("wiredtiger error: code=%d", int(code))
}

// ============================================================================
// CONNECTION OPERATIONS
// ============================================================================

func (s *cgoService) Open(home string, config string) error {
	chome := C.CString(home)
	cconfig := C.CString(config)
	defer C.free(unsafe.Pointer(chome))
	defer C.free(unsafe.Pointer(cconfig))
	var conn *C.WT_CONNECTION
	err := C.wt_open_wrap(chome, cconfig, &conn)
	if err != 0 {
		return fmt.Errorf("wiredtiger_open failed with error code %d", int(err))
	}
	if conn == nil {
		return errors.New("wiredtiger_open returned nil connection")
	}
	s.conn = conn
	return nil
}

func (s *cgoService) Close() error {
	if s.conn == nil {
		return nil
	}
	err := C.wt_close_wrap(s.conn)
	s.conn = nil
	if err != 0 {
		return fmt.Errorf("wiredtiger close failed with error code %d", int(err))
	}
	return nil
}

func (s *cgoService) CreateTable(name string, config string) error {
	if s.conn == nil {
		return errors.New("connection not open")
	}
	cname := C.CString(name)
	cconfig := C.CString(config)
	defer C.free(unsafe.Pointer(cname))
	defer C.free(unsafe.Pointer(cconfig))
	err := C.wt_create_wrap(s.conn, cname, cconfig)
	if err != 0 {
		return fmt.Errorf("wiredtiger create failed with error code %d", int(err))
	}
	return nil
}

func (s *cgoService) DeleteTable(name string) error {
	if s.conn == nil {
		return errors.New("connection not open")
	}
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	err := C.wt_drop_wrap(s.conn, cname)
	if err != 0 {
		if err == -31803 { // WT_NOTFOUND
			return ErrNotFound
		}
		if err == C.int(C.EBUSY) {
			return ErrBusy
		}
		return fmt.Errorf("wiredtiger drop failed with error code %d", int(err))
	}
	return nil
}

func (s *cgoService) TableExists(name string) (bool, error) {
	if s.conn == nil {
		return false, errors.New("connection not open")
	}
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	var exists C.int
	err := C.wt_table_exists(s.conn, cname, &exists)
	if err != 0 {
		return false, fmt.Errorf("wiredtiger table check failed with error code %d", int(err))
	}
	return exists != 0, nil
}

// ============================================================================
// STRING KEY/VALUE OPERATIONS
// ============================================================================

func (s *cgoService) PutString(table string, key string, value string) error {
	if s.conn == nil {
		return errors.New("connection not open")
	}
	curi := C.CString(table)
	ckey := C.CString(key)
	cval := C.CString(value)
	defer C.free(unsafe.Pointer(curi))
	defer C.free(unsafe.Pointer(ckey))
	defer C.free(unsafe.Pointer(cval))
	err := C.wt_put_str(s.conn, curi, ckey, cval)
	if err != 0 {
		return fmt.Errorf("wiredtiger put failed with error code %d", int(err))
	}
	return nil
}

func (s *cgoService) GetString(table string, key string) (string, bool, error) {
	if s.conn == nil {
		return "", false, errors.New("connection not open")
	}
	curi := C.CString(table)
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(curi))
	defer C.free(unsafe.Pointer(ckey))
	var cval *C.char
	err := C.wt_get_str(s.conn, curi, ckey, &cval)
	if err != 0 {
		return "", false, nil
	}
	return C.GoString(cval), true, nil
}

func (s *cgoService) DeleteString(table string, key string) error {
	if s.conn == nil {
		return errors.New("connection not open")
	}
	curi := C.CString(table)
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(curi))
	defer C.free(unsafe.Pointer(ckey))
	err := C.wt_del_str(s.conn, curi, ckey)
	if err != 0 {
		return fmt.Errorf("wiredtiger delete failed with error code %d", int(err))
	}
	return nil
}

func (s *cgoService) Exists(table string, key string) (bool, error) {
	if s.conn == nil {
		return false, errors.New("connection not open")
	}
	curi := C.CString(table)
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(curi))
	defer C.free(unsafe.Pointer(ckey))
	var found C.int
	err := C.wt_exists_str(s.conn, curi, ckey, &found)
	if err != 0 && err != C.int(-31804) {
		return false, fmt.Errorf("wiredtiger exists failed with error code %d", int(err))
	}
	return found == 1, nil
}

// Scan returns up to a given threshold (default 4096 if threshold is 0 or less) rows from the table.
// If threshold is provided as >0, use it. Otherwise, default to 4096.
func (s *cgoService) Scan(table string, threshold ...int) ([]KeyValuePair, error) {
	limit := 4096
	if len(threshold) > 0 && threshold[0] > 0 {
		limit = threshold[0]
	}
	return s.ScanBatch(table, 0, limit)
}

// ScanBatch enables batched scanning of results.
// Offset is currently not implemented due to WiredTiger API limitations and is for API compatibility.
// Limit sets the maximum number of results returned.
func (s *cgoService) ScanBatch(table string, offset int, limit int) ([]KeyValuePair, error) {
	if s.conn == nil {
		return nil, errors.New("connection not open")
	}
	if limit <= 0 {
		return []KeyValuePair{}, nil
	}
	curi := C.CString(table)
	defer C.free(unsafe.Pointer(curi))
	var vec C.wt_vec_t
	err := C.wt_scan_collect(s.conn, curi, C.int(limit), &vec)
	if err != 0 {
		return nil, fmt.Errorf("wiredtiger scan failed with error code %d", int(err))
	}
	n := int(vec.len)
	out := make([]KeyValuePair, 0, n)
	keys := (*[1 << 28]*C.char)(unsafe.Pointer(vec.keys))[:n:n]
	vals := (*[1 << 28]*C.char)(unsafe.Pointer(vec.vals))[:n:n]
	for i := 0; i < n; i++ {
		out = append(out, KeyValuePair{Key: C.GoString(keys[i]), Value: C.GoString(vals[i])})
	}
	C.wt_vec_free(vec)
	return out, nil
}

func (s *cgoService) ExistsBinary(table string, key []byte) (bool, error) {
	if s.conn == nil {
		return false, errors.New("connection not open")
	}
	if len(key) == 0 {
		return false, errors.New("key cannot be empty")
	}
	curi := C.CString(table)
	defer C.free(unsafe.Pointer(curi))

	var found C.int
	err := C.wt_exists_bin(s.conn, curi, (*C.uchar)(unsafe.Pointer(&key[0])), C.size_t(len(key)), &found)
	if err != 0 && err != C.int(-31804) {
		return false, fmt.Errorf("wiredtiger binary exists failed with error code %d", int(err))
	}
	return found == 1, nil
}

func (s *cgoService) ScanBinary(table string) ([]BinaryKeyValuePair, error) {
	if s.conn == nil {
		return nil, errors.New("connection not open")
	}
	curi := C.CString(table)
	defer C.free(unsafe.Pointer(curi))

	var vec C.wt_bin_vec_t
	err := C.wt_scan_bin_collect(s.conn, curi, C.int(4096), &vec)
	if err != 0 {
		return nil, fmt.Errorf("wiredtiger binary scan failed with error code %d", int(err))
	}

	n := int(vec.len)
	out := make([]BinaryKeyValuePair, 0, n)

	// Convert C arrays to Go slices
	keys := (*[1 << 28]*C.uchar)(unsafe.Pointer(vec.keys))[:n:n]
	vals := (*[1 << 28]*C.uchar)(unsafe.Pointer(vec.vals))[:n:n]
	keyLens := (*[1 << 28]C.size_t)(unsafe.Pointer(vec.key_lens))[:n:n]
	valLens := (*[1 << 28]C.size_t)(unsafe.Pointer(vec.val_lens))[:n:n]

	for i := 0; i < n; i++ {
		keyBytes := C.GoBytes(unsafe.Pointer(keys[i]), C.int(keyLens[i]))
		valBytes := C.GoBytes(unsafe.Pointer(vals[i]), C.int(valLens[i]))
		out = append(out, BinaryKeyValuePair{Key: keyBytes, Value: valBytes})
	}

	C.wt_bin_vec_free(vec)
	return out, nil
}

func (s *cgoService) SearchNearBinary(table string, probeKey []byte) ([]byte, []byte, int, bool, error) {
	if s.conn == nil {
		return nil, nil, 0, false, errors.New("connection not open")
	}
	if len(probeKey) == 0 {
		return nil, nil, 0, false, errors.New("probe key cannot be empty")
	}
	curi := C.CString(table)
	defer C.free(unsafe.Pointer(curi))

	var outKey C.WT_ITEM
	var outVal C.WT_ITEM
	var exact C.int

	err := C.wt_search_near_bin(s.conn, curi, (*C.uchar)(unsafe.Pointer(&probeKey[0])), C.size_t(len(probeKey)),
		&outKey, &outVal, &exact)
	if err != 0 {
		return nil, nil, 0, false, fmt.Errorf("wiredtiger binary search_near failed with error code %d", int(err))
	}

	// Copy C data to Go slices
	keyBytes := C.GoBytes(unsafe.Pointer(outKey.data), C.int(outKey.size))
	valBytes := C.GoBytes(unsafe.Pointer(outVal.data), C.int(outVal.size))
	C.free(outKey.data)
	C.free(outVal.data)

	return keyBytes, valBytes, int(exact), true, nil
}

func (s *cgoService) SearchNear(table string, probeKey string) (string, string, int, bool, error) {
	if s.conn == nil {
		return "", "", 0, false, errors.New("connection not open")
	}
	curi := C.CString(table)
	ckey := C.CString(probeKey)
	defer C.free(unsafe.Pointer(curi))
	defer C.free(unsafe.Pointer(ckey))
	var outKey *C.char
	var outVal *C.char
	var exact C.int
	err := C.wt_search_near_str(s.conn, curi, ckey, &outKey, &outVal, &exact)
	if err != 0 {
		return "", "", 0, false, fmt.Errorf("wiredtiger search_near failed with error code %d", int(err))
	}
	return C.GoString(outKey), C.GoString(outVal), int(exact), true, nil
}

// ============================================================================
// BINARY KEY/VALUE OPERATIONS
// ============================================================================

func (s *cgoService) PutBinary(table string, key []byte, value []byte) error {
	if s.conn == nil {
		return errors.New("connection not open")
	}
	if len(key) == 0 || len(value) == 0 {
		return errors.New("key and value cannot be empty")
	}
	curi := C.CString(table)
	defer C.free(unsafe.Pointer(curi))

	err := C.wt_put_bin(s.conn, curi, (*C.uchar)(unsafe.Pointer(&key[0])), C.size_t(len(key)),
		(*C.uchar)(unsafe.Pointer(&value[0])), C.size_t(len(value)))
	if merr := mapWiretigerError(err); merr != nil {
		return merr
	}
	return nil
}
func (s *cgoService) GetBinary(table string, key []byte) ([]byte, bool, error) {
	if s.conn == nil {
		return nil, false, errors.New("connection not open")
	}
	if len(key) == 0 {
		return nil, false, errors.New("key cannot be empty")
	}
	curi := C.CString(table)
	defer C.free(unsafe.Pointer(curi))

	var outVal C.WT_ITEM
	err := C.wt_get_bin(s.conn, curi, (*C.uchar)(unsafe.Pointer(&key[0])), C.size_t(len(key)), &outVal)
	if merr := mapWiretigerError(err); merr != nil {
		if errors.Is(merr, ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, merr
	}

	if outVal.data == nil {
		return nil, false, errors.New("wiredtiger returned null data pointer")
	}

	// Copy C data to Go slice
	result := C.GoBytes(unsafe.Pointer(outVal.data), C.int(outVal.size))
	C.free(outVal.data)

	return result, true, nil
}

func (s *cgoService) DeleteBinary(table string, key []byte) error {
	if s.conn == nil {
		return errors.New("connection not open")
	}
	if len(key) == 0 {
		return errors.New("key cannot be empty")
	}
	curi := C.CString(table)
	defer C.free(unsafe.Pointer(curi))

	err := C.wt_del_bin(s.conn, curi, (*C.uchar)(unsafe.Pointer(&key[0])), C.size_t(len(key)))
	if err != 0 {
		return fmt.Errorf("wiredtiger binary delete failed with error code %d", int(err))
	}
	return nil
}

// PutBinaryWithStringKey is a convenience function for composite keys
// Useful for index operations where key is composite of field values + doc ID
func (s *cgoService) PutBinaryWithStringKey(table string, stringKey string, value []byte) error {
	keyBytes := []byte(stringKey)
	return s.PutBinary(table, keyBytes, value)
}

// GetBinaryWithStringKey is a convenience function
// Retrieves binary value using a string key
func (s *cgoService) GetBinaryWithStringKey(table string, stringKey string) ([]byte, bool, error) {
	keyBytes := []byte(stringKey)
	return s.GetBinary(table, keyBytes)
}

// DeleteBinaryWithStringKey is a convenience function
// Deletes a key-value pair using a string key
func (s *cgoService) DeleteBinaryWithStringKey(table string, stringKey string) error {
	keyBytes := []byte(stringKey)
	return s.DeleteBinary(table, keyBytes)
}

// ============================================================================
// RANGE SCAN OPERATIONS
// ============================================================================
type stringRangeCursor struct {
	ctx       *C.wt_range_ctx_t
	err       error
	closed    bool
	valid     bool
	firstCall bool

	// batchBuffer holds a batch of key-value pairs from the C layer.
	// The format is [key1_len (u32)][key1_data][val1_len (u32)][val1_data]...
	batchBuffer []byte
	// readOffset is the current reading position in batchBuffer.
	readOffset int

	// Current key and value parsed from the batch.
	currKey string
	currVal string
}

func (c *stringRangeCursor) Next() bool {
	if c.closed || c.err != nil || c.ctx == nil {
		c.valid = false
		return false
	}

	// If the buffer is fully read, fetch the next batch.
	if c.readOffset >= len(c.batchBuffer) {
		if err := c.fetchNextBatch(); err != nil {
			c.err = err
			c.valid = false
			return false
		}
		// If the new batch is empty, we're done.
		if len(c.batchBuffer) == 0 {
			c.valid = false
			return false
		}
	}

	// Read the next key-value pair from the batch buffer.
	// Each entry is length-prefixed.
	if err := c.readNextKV(); err != nil {
		c.err = err
		c.valid = false
		return false
	}

	c.valid = true
	return true
}

func (c *stringRangeCursor) fetchNextBatch() error {
	const batchSize = 1000
	var cBuf *C.char
	var cBufLen C.int
	var numFetched C.int

	errCode := C.wt_range_scan_next_batch(c.ctx, batchSize, &cBuf, &cBufLen, &numFetched)
	if errCode != 0 {
		return errors.New("range scan fetchNextBatch failed")
	}
	defer C.wt_free_batch_buf(cBuf) // Free the C buffer after copying.

	if numFetched == 0 {
		c.batchBuffer = nil
		c.readOffset = 0
		return nil
	}

	// Copy the data from C memory to a Go-managed byte slice.
	c.batchBuffer = C.GoBytes(unsafe.Pointer(cBuf), cBufLen)
	c.readOffset = 0
	return nil
}

// readNextKV parses the next key and value from the batchBuffer.
func (c *stringRangeCursor) readNextKV() error {
	buf := c.batchBuffer
	offset := c.readOffset

	// Ensure there's enough data for key length.
	if len(buf)-offset < 4 {
		return errors.New("incomplete batch: could not read key length")
	}
	keyLen := int(binary.LittleEndian.Uint32(buf[offset:]))
	offset += 4

	// Ensure there's enough data for the key.
	if len(buf)-offset < keyLen {
		return errors.New("incomplete batch: could not read key")
	}
	c.currKey = string(buf[offset : offset+keyLen])
	offset += keyLen

	// Ensure there's enough data for value length.
	if len(buf)-offset < 4 {
		return errors.New("incomplete batch: could not read value length")
	}
	valLen := int(binary.LittleEndian.Uint32(buf[offset:]))
	offset += 4

	// Ensure there's enough data for the value.
	if len(buf)-offset < valLen {
		return errors.New("incomplete batch: could not read value")
	}
	c.currVal = string(buf[offset : offset+valLen])
	offset += valLen

	c.readOffset = offset
	return nil
}

func (c *stringRangeCursor) CurrentString() (string, string, error) {
	if !c.valid {
		return "", "", errors.New("cursor not positioned on a valid record")
	}
	if c.err != nil {
		return "", "", c.err
	}
	return c.currKey, c.currVal, nil
}

func (c *stringRangeCursor) Err() error { return c.err }
func (c *stringRangeCursor) Close() error {
	if c.closed || c.ctx == nil {
		return nil
	}
	C.wt_range_scan_close(c.ctx)
	c.closed = true
	return nil
}
func (c *stringRangeCursor) Valid() bool { return c.valid }

// ScanRange creates a cursor for iterating over string keys in the range [startKey, endKey)
func (s *cgoService) ScanRange(table, startKey, endKey string) (StringRangeCursor, error) {
	if s.conn == nil {
		return nil, errors.New("connection not open")
	}

	// Print incoming keys for debugging
	fmt.Printf("[ScanRange] table: %s, startKey: %s, endKey: %s\n", table, startKey, endKey)

	ctable := C.CString(table)
	// Validate that keys are not empty to prevent WiredTiger panic
	if startKey == "" || endKey == "" {
		return nil, errors.New("start and end keys cannot be empty")
	}

	cstart := C.CString(startKey)
	cend := C.CString(endKey)
	defer C.free(unsafe.Pointer(ctable))
	defer C.free(unsafe.Pointer(cstart))
	defer C.free(unsafe.Pointer(cend))
	ctx := C.wt_range_scan_init_str(s.conn, ctable, cstart, cend)
	fmt.Printf("ctx: %v\n", ctx)
	if ctx == nil {
		return nil, errors.New("failed to initialize string range scan")
	}
	out := &stringRangeCursor{
		ctx:         ctx,
		valid:       true,
		firstCall:   true,
		batchBuffer: nil,
		readOffset:  0,
		currKey:     "",
		currVal:     "",
	}
	return out, nil
}

// ============================================================================
// BINARY RANGE SCAN
// ============================================================================

func (s *cgoService) ScanRangeBinary(table string, startKey, endKey []byte) (BinaryRangeCursor, error) {
	if s.conn == nil {
		return nil, errors.New("connection not open")
	}

	ctable := C.CString(table)
	defer C.free(unsafe.Pointer(ctable))

	var cStartKey, cEndKey C.WT_ITEM
	if len(startKey) > 0 {
		pStartKey := C.CBytes(startKey)
		defer C.free(pStartKey)
		cStartKey.data = pStartKey
		cStartKey.size = C.size_t(len(startKey))
	}
	if len(endKey) > 0 {
		pEndKey := C.CBytes(endKey)
		defer C.free(pEndKey)
		cEndKey.data = pEndKey
		cEndKey.size = C.size_t(len(endKey))
	}

	ctx := C.wt_range_scan_init_bin(s.conn, ctable, &cStartKey, &cEndKey)
	if ctx == nil {
		return nil, errors.New("failed to initialize binary range scan")
	}

	return &binaryRangeCursor{
		ctx:          ctx,
		valid:        ctx.valid == 1,
		maxBatchSize: 24 * 1024, // 24KB - fits comfortably in L1 cache (32KB)
	}, nil
}

type binaryRangeCursor struct {
	ctx *C.wt_range_ctx_bin_t
	err error

	buf  []byte // batch buffer
	off  int    // offset in buf
	left int    // remaining records in current batch

	currKey []byte
	currVal []byte

	maxBatchSize int // Maximum batch size (default 24kb)
	valid        bool
}

func (c *binaryRangeCursor) Next() bool {
	if c.err != nil || c.ctx == nil {
		c.valid = false
		return false
	}
	if c.left == 0 {
		if err := c.fetchBatch(); err != nil {
			c.err = err
			c.valid = false
			return false
		}
		if c.left == 0 { // no more data
			c.valid = false
			return false
		}
	}
	// parse one record
	if c.off+4 > len(c.buf) {
		c.err = errors.New("incomplete batch: key len")
		c.valid = false
		return false
	}
	klen := int(binary.LittleEndian.Uint32(c.buf[c.off:]))
	c.off += 4
	if c.off+klen+4 > len(c.buf) {
		c.err = errors.New("incomplete batch: key")
		c.valid = false
		return false
	}
	c.currKey = c.buf[c.off : c.off+klen]
	c.off += klen
	vlen := int(binary.LittleEndian.Uint32(c.buf[c.off:]))
	c.off += 4
	if c.off+vlen > len(c.buf) {
		c.err = errors.New("incomplete batch: value")
		c.valid = false
		return false
	}
	c.currVal = c.buf[c.off : c.off+vlen]
	c.off += vlen

	c.left--
	c.valid = true
	return true
}

func (c *binaryRangeCursor) fetchBatch() error {
	maxBuf := c.maxBatchSize
	var cBuf *C.uchar
	var cBufLen C.int
	var num C.int
	code := C.wt_range_scan_next_batch_bin(c.ctx, C.size_t(maxBuf), &cBuf, &cBufLen, &num)
	if code != 0 {
		return fmt.Errorf("range batch failed: %d", int(code))
	}
	if num == 0 || cBuf == nil || cBufLen <= 0 {
		c.buf = nil
		c.off = 0
		c.left = 0
		return nil
	}
	// copy and free
	c.buf = C.GoBytes(unsafe.Pointer(cBuf), cBufLen)
	C.wt_free_batch_buf_bin(cBuf)
	if len(c.buf) < 4 {
		return errors.New("incomplete batch header")
	}
	c.left = int(binary.LittleEndian.Uint32(c.buf[0:4]))
	c.off = 4
	return nil
}

func (c *binaryRangeCursor) Current() ([]byte, []byte, error) {
	if !c.valid {
		return nil, nil, errors.New("cursor not on record")
	}
	return c.currKey, c.currVal, nil
}

func (c *binaryRangeCursor) Err() error { return c.err }

func (c *binaryRangeCursor) Close() error {
	if c.ctx != nil {
		C.wt_range_scan_close_bin(c.ctx)
		c.ctx = nil
	}
	return nil
}

func (c *binaryRangeCursor) Valid() bool { return c.valid }

func (c *binaryRangeCursor) SetBatchSize(size int) {
	if size > 0 {
		c.maxBatchSize = size
	}
}

// GetBatchSize returns the current batch size
func (c *binaryRangeCursor) GetBatchSize() int {
	return c.maxBatchSize
}

// ============================================================================
// BATCH WRITER IMPLEMENTATION
// ============================================================================

type cgoBatchWriter struct {
	ctx      *C.wt_batch_ctx_t
	conn     *C.WT_CONNECTION
	table    string
	isBinary bool
	closed   bool
}

// NewBatchWriter creates a new batch writer for efficient bulk inserts.
// The writer keeps a single session and cursor open for all writes.
func (s *cgoService) NewBatchWriter(table string) (BatchWriter, error) {
	if s.conn == nil {
		return nil, errors.New("connection not open")
	}

	ctable := C.CString(table)
	defer C.free(unsafe.Pointer(ctable))

	// Determine if table uses binary or string format based on table name
	// Tables with binary format: collection tables, _catalog, _stats
	// Tables with string format: label_docID
	isBinary := table != "table:label_docID"

	var isBinaryC C.int = 0
	if isBinary {
		isBinaryC = 1
	}

	ctx := C.wt_batch_open(s.conn, ctable, isBinaryC)
	if ctx == nil {
		return nil, fmt.Errorf("failed to open batch writer for table %s", table)
	}

	return &cgoBatchWriter{
		ctx:      ctx,
		conn:     s.conn,
		table:    table,
		isBinary: isBinary,
		closed:   false,
	}, nil
}

func (w *cgoBatchWriter) PutBinary(key, value []byte) error {
	if w.closed || w.ctx == nil {
		return errors.New("batch writer is closed")
	}
	if len(key) == 0 || len(value) == 0 {
		return errors.New("key and value cannot be empty")
	}

	err := C.wt_batch_put_bin(w.ctx,
		(*C.uchar)(unsafe.Pointer(&key[0])), C.size_t(len(key)),
		(*C.uchar)(unsafe.Pointer(&value[0])), C.size_t(len(value)))

	if err != 0 {
		return fmt.Errorf("batch put binary failed: %d", int(err))
	}
	return nil
}

func (w *cgoBatchWriter) PutString(key, value string) error {
	if w.closed || w.ctx == nil {
		return errors.New("batch writer is closed")
	}

	ckey := C.CString(key)
	cval := C.CString(value)
	defer C.free(unsafe.Pointer(ckey))
	defer C.free(unsafe.Pointer(cval))

	err := C.wt_batch_put_str(w.ctx, ckey, cval)
	if err != 0 {
		return fmt.Errorf("batch put string failed: %d", int(err))
	}
	return nil
}

func (w *cgoBatchWriter) Commit() error {
	// WiredTiger auto-commits on cursor operations when not in explicit transaction
	// For now, this is a no-op but could be extended to use explicit transactions
	return nil
}

func (w *cgoBatchWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	if w.ctx != nil {
		err := C.wt_batch_close(w.ctx)
		w.ctx = nil
		if err != 0 {
			return fmt.Errorf("batch close failed: %d", int(err))
		}
	}
	return nil
}
