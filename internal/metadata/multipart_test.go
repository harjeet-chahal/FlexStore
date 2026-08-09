package metadata

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

// uploadPart writes one part with the given chunk sizes and completes it.
func uploadPart(t *testing.T, ctx context.Context, s *Store, uploadID uuid.UUID, partNumber int32, nodes []string, sizes ...int64) uuid.UUID {
	t.Helper()

	part, err := s.BeginPart(ctx, uploadID, partNumber)
	if err != nil {
		t.Fatalf("BeginPart %d: %v", partNumber, err)
	}
	var total int64
	for i, size := range sizes {
		chunkID, err := s.AllocateChunk(ctx, ChunkOwner{PartID: &part.ID}, int32(i), size, nodes)
		if err != nil {
			t.Fatalf("AllocateChunk part=%d idx=%d: %v", partNumber, i, err)
		}
		// Distinct checksum per (part, chunk) so mis-ordering is detectable.
		if _, err := s.CommitChunk(ctx, chunkID, fakeChecksum(int(partNumber)*100+i), size, nodes, nil, 1); err != nil {
			t.Fatalf("CommitChunk part=%d idx=%d: %v", partNumber, i, err)
		}
		total += size
	}
	if err := s.CompletePart(ctx, part.ID, total, int32(len(sizes)), fakeChecksum(int(partNumber))); err != nil {
		t.Fatalf("CompletePart %d: %v", partNumber, err)
	}
	return part.ID
}

func TestMultipartUploadAssemblesPartsInOrder(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt, key := bucket(t), "multipart/video.mp4"

	mu, err := s.CreateMultipartUpload(ctx, bkt, key, "video/mp4", 8<<20)
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if mu.State != MultipartInProgress {
		t.Fatalf("state = %s, want IN_PROGRESS", mu.State)
	}

	// Upload parts out of order on purpose: assembly must sort by part number,
	// not by upload order.
	uploadPart(t, ctx, s, mu.ID, 3, nodes, 300, 301)
	uploadPart(t, ctx, s, mu.ID, 1, nodes, 100, 101)
	uploadPart(t, ctx, s, mu.ID, 2, nodes, 200)

	obj, err := s.CompleteMultipartUpload(ctx, mu.ID, nil)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	if obj.State != ObjectComplete {
		t.Fatalf("state = %s, want COMPLETE", obj.State)
	}
	wantSize := int64(100 + 101 + 200 + 300 + 301)
	if obj.SizeBytes != wantSize {
		t.Fatalf("size = %d, want %d", obj.SizeBytes, wantSize)
	}
	if obj.ChunkCount != 5 {
		t.Fatalf("chunk count = %d, want 5", obj.ChunkCount)
	}
	if obj.ContentType != "video/mp4" {
		t.Errorf("ContentType = %q", obj.ContentType)
	}

	_, chunks, err := s.GetObject(ctx, bkt, key)
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	// Chunk indices must be a contiguous 0..4 in (part, chunk) order, and the
	// sizes must follow the part order -- this is the assembly correctness
	// property, and a bug here silently scrambles the object's bytes.
	wantSizes := []int64{100, 101, 200, 300, 301}
	if len(chunks) != len(wantSizes) {
		t.Fatalf("got %d chunks, want %d", len(chunks), len(wantSizes))
	}
	for i, c := range chunks {
		if c.Index != int32(i) {
			t.Fatalf("position %d has index %d; renumbering is wrong", i, c.Index)
		}
		if c.SizeBytes != wantSizes[i] {
			t.Fatalf("chunk %d size = %d, want %d (parts were assembled out of order)",
				i, c.SizeBytes, wantSizes[i])
		}
		if len(c.Replicas) != 3 {
			t.Errorf("chunk %d has %d replicas, want 3", i, len(c.Replicas))
		}
	}
}

func TestCompleteMultipartUploadValidatesAClientManifest(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)

	mu, _ := s.CreateMultipartUpload(ctx, bucket(t), "manifest.bin", "", 8<<20)
	uploadPart(t, ctx, s, mu.ID, 1, nodes, 100)
	uploadPart(t, ctx, s, mu.ID, 2, nodes, 200)

	// The client thinks it uploaded three parts. Silently completing with two
	// would hand back a truncated object.
	_, err := s.CompleteMultipartUpload(ctx, mu.ID, []CompletedPartRef{
		{PartNumber: 1}, {PartNumber: 2}, {PartNumber: 3},
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict for a part-count mismatch, got %v", err)
	}

	// A wrong ETag must also be caught.
	_, err = s.CompleteMultipartUpload(ctx, mu.ID, []CompletedPartRef{
		{PartNumber: 1, ETag: fakeChecksum(1)},
		{PartNumber: 2, ETag: "0000000000000000000000000000000000000000000000000000000000000000"},
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict for an etag mismatch, got %v", err)
	}

	// The correct manifest succeeds, including quoted ETags as S3 clients send.
	obj, err := s.CompleteMultipartUpload(ctx, mu.ID, []CompletedPartRef{
		{PartNumber: 1, ETag: `"` + fakeChecksum(1) + `"`},
		{PartNumber: 2, ETag: fakeChecksum(2)},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload with a correct manifest: %v", err)
	}
	if obj.SizeBytes != 300 {
		t.Fatalf("size = %d, want 300", obj.SizeBytes)
	}
}

func TestReuploadingAPartReplacesIt(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt, key := bucket(t), "reupload.bin"

	mu, _ := s.CreateMultipartUpload(ctx, bkt, key, "", 8<<20)
	firstPartID := uploadPart(t, ctx, s, mu.ID, 1, nodes, 100)
	// Re-upload part 1 with a different size.
	uploadPart(t, ctx, s, mu.ID, 1, nodes, 999)
	uploadPart(t, ctx, s, mu.ID, 2, nodes, 50)

	obj, err := s.CompleteMultipartUpload(ctx, mu.ID, nil)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	if obj.SizeBytes != 999+50 {
		t.Fatalf("size = %d, want %d; the superseded part was used", obj.SizeBytes, 999+50)
	}

	// The superseded part's chunks must be queued for reclamation.
	var queued int
	err = s.Pool().QueryRow(ctx, `
		SELECT COUNT(*) FROM chunk_deletions d
		JOIN chunks c ON c.id = d.chunk_id
		WHERE c.part_id = $1`, firstPartID).Scan(&queued)
	if err != nil {
		t.Fatalf("counting deletions: %v", err)
	}
	if queued == 0 {
		t.Fatal("the replaced part's chunks were not queued for deletion; they would leak")
	}
}

func TestAbortMultipartUpload(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)

	mu, _ := s.CreateMultipartUpload(ctx, bucket(t), "aborted-mpu.bin", "", 8<<20)
	uploadPart(t, ctx, s, mu.ID, 1, nodes, 100, 200)

	if err := s.AbortMultipartUpload(ctx, mu.ID); err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}

	after, err := s.GetMultipartUpload(ctx, mu.ID)
	if err != nil {
		t.Fatalf("GetMultipartUpload: %v", err)
	}
	if after.State != MultipartAborted {
		t.Fatalf("state = %s, want ABORTED", after.State)
	}

	// 2 chunks x 3 replicas.
	var queued int
	if err := s.Pool().QueryRow(ctx, `
		SELECT COUNT(*) FROM chunk_deletions d
		JOIN chunks c ON c.id = d.chunk_id
		JOIN multipart_parts p ON p.id = c.part_id
		WHERE p.upload_id = $1`, mu.ID).Scan(&queued); err != nil {
		t.Fatalf("counting deletions: %v", err)
	}
	if queued != 6 {
		t.Fatalf("%d deletion jobs queued, want 6", queued)
	}

	// Aborting twice is a no-op, not an error.
	if err := s.AbortMultipartUpload(ctx, mu.ID); err != nil {
		t.Fatalf("second AbortMultipartUpload: %v", err)
	}
	// And no further parts may be added.
	if _, err := s.BeginPart(ctx, mu.ID, 2); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict when adding a part to an aborted upload, got %v", err)
	}
}

func TestAbortUnknownMultipartUpload(t *testing.T) {
	s, ctx := testStore(t)
	if err := s.AbortMultipartUpload(ctx, uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestCompleteMultipartUploadRequiresAtLeastOnePart(t *testing.T) {
	s, ctx := testStore(t)
	mu, _ := s.CreateMultipartUpload(ctx, bucket(t), "empty-mpu.bin", "", 8<<20)

	if _, err := s.CompleteMultipartUpload(ctx, mu.ID, nil); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict for an upload with no parts, got %v", err)
	}
}

func TestCompletePartRejectsUncommittedChunks(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)

	mu, _ := s.CreateMultipartUpload(ctx, bucket(t), "partial-part.bin", "", 8<<20)
	part, err := s.BeginPart(ctx, mu.ID, 1)
	if err != nil {
		t.Fatalf("BeginPart: %v", err)
	}
	// Allocate but never commit.
	if _, err := s.AllocateChunk(ctx, ChunkOwner{PartID: &part.ID}, 0, 100, nodes); err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}

	err = s.CompletePart(ctx, part.ID, 100, 1, fakeChecksum(1))
	if !errors.Is(err, ErrDurabilityNotMet) {
		t.Fatalf("expected ErrDurabilityNotMet, got %v", err)
	}
}

func TestBeginPartRejectsOutOfRangePartNumbers(t *testing.T) {
	s, ctx := testStore(t)
	mu, _ := s.CreateMultipartUpload(ctx, bucket(t), "range.bin", "", 8<<20)

	for _, n := range []int32{0, -1, 10001} {
		if _, err := s.BeginPart(ctx, mu.ID, n); !errors.Is(err, ErrConflict) {
			t.Errorf("BeginPart(%d) should be rejected, got %v", n, err)
		}
	}
}

func TestMultipartOverwritesAnExistingObject(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt, key := bucket(t), "overwrite-via-mpu.bin"

	firstID := putObject(t, ctx, s, bkt, key, nodes, 100)

	mu, _ := s.CreateMultipartUpload(ctx, bkt, key, "", 8<<20)
	uploadPart(t, ctx, s, mu.ID, 1, nodes, 500)
	obj, err := s.CompleteMultipartUpload(ctx, mu.ID, nil)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}

	current, err := s.HeadObject(ctx, bkt, key)
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if current.ID != obj.ID {
		t.Fatalf("current version is %s, want the multipart object %s", current.ID, obj.ID)
	}
	old, _ := s.GetObjectByID(ctx, firstID)
	if old.State != ObjectDeleting {
		t.Fatalf("the superseded single-shot version is %s, want DELETING", old.State)
	}
}

func TestMultipartETagShapeDistinguishesMultipartObjects(t *testing.T) {
	// Same shape as S3's ("<digest>-<n>") so clients can tell them apart, but
	// SHA-256 based -- FlexStore does not claim S3 ETag compatibility.
	one := MultipartETag([]string{"a"})
	three := MultipartETag([]string{"a", "b", "c"})

	if one == three {
		t.Fatal("different part sets produced the same etag")
	}
	if got := three[len(three)-2:]; got != "-3" {
		t.Fatalf("etag %q does not end with the part count", three)
	}
	// Quoting must not change the value, since clients echo quoted ETags back.
	if MultipartETag([]string{`"a"`, `"b"`, `"c"`}) != three {
		t.Fatal("quoted and unquoted part etags produced different results")
	}
	// Deterministic.
	if MultipartETag([]string{"a", "b", "c"}) != three {
		t.Fatal("MultipartETag is not deterministic")
	}
}

func TestListParts(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)

	mu, _ := s.CreateMultipartUpload(ctx, bucket(t), "list-parts.bin", "", 8<<20)
	uploadPart(t, ctx, s, mu.ID, 2, nodes, 200)
	uploadPart(t, ctx, s, mu.ID, 1, nodes, 100)

	parts, err := s.ListParts(ctx, mu.ID)
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	if len(parts) != 2 {
		t.Fatalf("got %d parts, want 2", len(parts))
	}
	if parts[0].PartNumber != 1 || parts[1].PartNumber != 2 {
		t.Fatalf("parts are not in part-number order: %d then %d",
			parts[0].PartNumber, parts[1].PartNumber)
	}
	if parts[0].SizeBytes != 100 || parts[1].SizeBytes != 200 {
		t.Fatalf("unexpected part sizes: %d, %d", parts[0].SizeBytes, parts[1].SizeBytes)
	}
}
