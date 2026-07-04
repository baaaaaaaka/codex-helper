package teams

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const (
	maxTeamsMathFontFaces    = 64
	maxTeamsMathFontTables   = 4096
	maxTeamsMathCmapTables   = 256
	maxTeamsMathCmapBytes    = 4 * 1024 * 1024
	maxTeamsMathCmapSubtable = 256
)

type teamsMathCmapSubtable struct {
	format uint16
	data   []byte
}

func loadTeamsMathFontCmaps(path string) ([]teamsMathCmapSubtable, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open font: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat font: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxTeamsMathFontDownloadBytes {
		return nil, fmt.Errorf("font size %d is outside the supported range", info.Size())
	}
	faceOffsets, err := teamsMathFontFaceOffsets(file, info.Size())
	if err != nil {
		return nil, err
	}
	var cmaps []teamsMathCmapSubtable
	for _, offset := range faceOffsets {
		faceCmaps, faceErr := loadTeamsMathFaceCmaps(file, info.Size(), offset)
		if faceErr != nil {
			return nil, faceErr
		}
		cmaps = append(cmaps, faceCmaps...)
		if len(cmaps) > maxTeamsMathCmapSubtable {
			return nil, fmt.Errorf("font exposes too many cmap subtables")
		}
	}
	if len(cmaps) == 0 {
		return nil, fmt.Errorf("font has no supported Unicode cmap")
	}
	return cmaps, nil
}

func teamsMathFontFaceOffsets(file io.ReaderAt, size int64) ([]int64, error) {
	header := make([]byte, 12)
	if err := readTeamsMathFontAt(file, header, 0, size); err != nil {
		return nil, err
	}
	if string(header[:4]) != "ttcf" {
		if !validTeamsMathSFNTSignature(header[:4]) {
			return nil, fmt.Errorf("unsupported font signature %q", header[:4])
		}
		return []int64{0}, nil
	}
	count := int(binary.BigEndian.Uint32(header[8:12]))
	if count <= 0 || count > maxTeamsMathFontFaces {
		return nil, fmt.Errorf("font collection face count %d is invalid", count)
	}
	data := make([]byte, count*4)
	if err := readTeamsMathFontAt(file, data, 12, size); err != nil {
		return nil, err
	}
	offsets := make([]int64, 0, count)
	for index := 0; index < count; index++ {
		offset := int64(binary.BigEndian.Uint32(data[index*4 : index*4+4]))
		if offset < 0 || offset+12 > size {
			return nil, fmt.Errorf("font collection face offset %d is out of bounds", offset)
		}
		offsets = append(offsets, offset)
	}
	return offsets, nil
}

func validTeamsMathSFNTSignature(signature []byte) bool {
	if len(signature) != 4 {
		return false
	}
	return binary.BigEndian.Uint32(signature) == 0x00010000 || string(signature) == "OTTO" || string(signature) == "true" || string(signature) == "typ1"
}

func loadTeamsMathFaceCmaps(file io.ReaderAt, size int64, faceOffset int64) ([]teamsMathCmapSubtable, error) {
	header := make([]byte, 12)
	if err := readTeamsMathFontAt(file, header, faceOffset, size); err != nil {
		return nil, err
	}
	if !validTeamsMathSFNTSignature(header[:4]) {
		return nil, fmt.Errorf("unsupported face signature %q", header[:4])
	}
	count := int(binary.BigEndian.Uint16(header[4:6]))
	if count <= 0 || count > maxTeamsMathFontTables {
		return nil, fmt.Errorf("font table count %d is invalid", count)
	}
	directory := make([]byte, count*16)
	if err := readTeamsMathFontAt(file, directory, faceOffset+12, size); err != nil {
		return nil, err
	}
	for index := 0; index < count; index++ {
		record := directory[index*16 : index*16+16]
		if string(record[:4]) != "cmap" {
			continue
		}
		offset := int64(binary.BigEndian.Uint32(record[8:12]))
		length := int64(binary.BigEndian.Uint32(record[12:16]))
		if length < 4 || length > maxTeamsMathCmapBytes || offset < 0 || offset > size-length {
			return nil, fmt.Errorf("font cmap offset or length is invalid")
		}
		data := make([]byte, length)
		if err := readTeamsMathFontAt(file, data, offset, size); err != nil {
			return nil, err
		}
		return parseTeamsMathCmapTable(data)
	}
	return nil, fmt.Errorf("font face has no cmap table")
}

func parseTeamsMathCmapTable(table []byte) ([]teamsMathCmapSubtable, error) {
	if len(table) < 4 || binary.BigEndian.Uint16(table[:2]) != 0 {
		return nil, fmt.Errorf("font cmap header is invalid")
	}
	count := int(binary.BigEndian.Uint16(table[2:4]))
	if count <= 0 || count > maxTeamsMathCmapTables || 4+count*8 > len(table) {
		return nil, fmt.Errorf("font cmap encoding count is invalid")
	}
	seen := make(map[uint32]bool)
	cmaps := make([]teamsMathCmapSubtable, 0, count)
	for index := 0; index < count; index++ {
		record := table[4+index*8 : 12+index*8]
		platform := binary.BigEndian.Uint16(record[:2])
		encoding := binary.BigEndian.Uint16(record[2:4])
		if platform != 0 && !(platform == 3 && (encoding == 1 || encoding == 10)) {
			continue
		}
		offset := binary.BigEndian.Uint32(record[4:8])
		if seen[offset] {
			continue
		}
		seen[offset] = true
		if uint64(offset)+2 > uint64(len(table)) {
			return nil, fmt.Errorf("font cmap subtable offset is out of bounds")
		}
		format := binary.BigEndian.Uint16(table[offset : offset+2])
		length, ok := teamsMathCmapSubtableLength(table[offset:], format)
		if !ok {
			continue
		}
		if length <= 0 || length > maxTeamsMathCmapBytes || uint64(offset)+uint64(length) > uint64(len(table)) {
			return nil, fmt.Errorf("font cmap format %d length is invalid", format)
		}
		cmaps = append(cmaps, teamsMathCmapSubtable{format: format, data: table[offset : uint64(offset)+uint64(length)]})
	}
	if len(cmaps) == 0 {
		return nil, fmt.Errorf("font has no supported Unicode cmap subtable")
	}
	return cmaps, nil
}

func teamsMathCmapSubtableLength(data []byte, format uint16) (int, bool) {
	switch format {
	case 0, 4, 6:
		if len(data) < 4 {
			return 0, false
		}
		return int(binary.BigEndian.Uint16(data[2:4])), true
	case 10, 12, 13:
		if len(data) < 8 {
			return 0, false
		}
		length := binary.BigEndian.Uint32(data[4:8])
		if uint64(length) > uint64(^uint(0)>>1) {
			return 0, false
		}
		return int(length), true
	default:
		return 0, false
	}
}

func teamsMathCmapCoverage(cmaps []teamsMathCmapSubtable, values []rune) map[rune]bool {
	coverage := make(map[rune]bool, len(values))
	for _, value := range values {
		coverage[value] = false
		if value < 0 {
			continue
		}
		for _, cmap := range cmaps {
			if teamsMathCmapHasRune(cmap, uint32(value)) {
				coverage[value] = true
				break
			}
		}
	}
	return coverage
}

func teamsMathCmapHasRune(cmap teamsMathCmapSubtable, value uint32) bool {
	switch cmap.format {
	case 0:
		return value <= 0xff && len(cmap.data) >= 262 && cmap.data[6+value] != 0
	case 4:
		return teamsMathCmapFormat4HasRune(cmap.data, value)
	case 6:
		if value > 0xffff || len(cmap.data) < 10 {
			return false
		}
		first := uint32(binary.BigEndian.Uint16(cmap.data[6:8]))
		count := uint32(binary.BigEndian.Uint16(cmap.data[8:10]))
		if value < first || value-first >= count {
			return false
		}
		position := 10 + int(value-first)*2
		return position+2 <= len(cmap.data) && binary.BigEndian.Uint16(cmap.data[position:position+2]) != 0
	case 10:
		if len(cmap.data) < 20 {
			return false
		}
		first := binary.BigEndian.Uint32(cmap.data[12:16])
		count := binary.BigEndian.Uint32(cmap.data[16:20])
		if value < first || value-first >= count {
			return false
		}
		position := uint64(20) + uint64(value-first)*2
		return position+2 <= uint64(len(cmap.data)) && binary.BigEndian.Uint16(cmap.data[position:position+2]) != 0
	case 12, 13:
		return teamsMathCmapGroupedHasRune(cmap.data, value, cmap.format == 13)
	default:
		return false
	}
}

func teamsMathCmapFormat4HasRune(data []byte, value uint32) bool {
	if value > 0xffff || len(data) < 16 {
		return false
	}
	segmentCountX2 := int(binary.BigEndian.Uint16(data[6:8]))
	if segmentCountX2 == 0 || segmentCountX2%2 != 0 {
		return false
	}
	segmentCount := segmentCountX2 / 2
	endBase := 14
	startBase := endBase + segmentCount*2 + 2
	deltaBase := startBase + segmentCount*2
	rangeBase := deltaBase + segmentCount*2
	if rangeBase+segmentCount*2 > len(data) {
		return false
	}
	left, right := 0, segmentCount
	for left < right {
		middle := left + (right-left)/2
		end := uint32(binary.BigEndian.Uint16(data[endBase+middle*2 : endBase+middle*2+2]))
		if end < value {
			left = middle + 1
		} else {
			right = middle
		}
	}
	if left >= segmentCount {
		return false
	}
	start := uint32(binary.BigEndian.Uint16(data[startBase+left*2 : startBase+left*2+2]))
	if value < start {
		return false
	}
	delta := int32(int16(binary.BigEndian.Uint16(data[deltaBase+left*2 : deltaBase+left*2+2])))
	rangePosition := rangeBase + left*2
	rangeOffset := int(binary.BigEndian.Uint16(data[rangePosition : rangePosition+2]))
	if rangeOffset == 0 {
		return uint16(int32(value)+delta) != 0
	}
	glyphPosition := rangePosition + rangeOffset + int(value-start)*2
	if glyphPosition < 0 || glyphPosition+2 > len(data) {
		return false
	}
	glyph := binary.BigEndian.Uint16(data[glyphPosition : glyphPosition+2])
	if glyph == 0 {
		return false
	}
	return uint16(int32(glyph)+delta) != 0
}

func teamsMathCmapGroupedHasRune(data []byte, value uint32, constant bool) bool {
	if len(data) < 16 {
		return false
	}
	count := uint64(binary.BigEndian.Uint32(data[12:16]))
	if 16+count*12 > uint64(len(data)) {
		return false
	}
	left, right := uint64(0), count
	for left < right {
		middle := left + (right-left)/2
		position := 16 + middle*12
		end := binary.BigEndian.Uint32(data[position+4 : position+8])
		if end < value {
			left = middle + 1
		} else {
			right = middle
		}
	}
	if left >= count {
		return false
	}
	position := 16 + left*12
	start := binary.BigEndian.Uint32(data[position : position+4])
	end := binary.BigEndian.Uint32(data[position+4 : position+8])
	if value < start || value > end {
		return false
	}
	glyph := binary.BigEndian.Uint32(data[position+8 : position+12])
	if !constant {
		glyph += value - start
	}
	return glyph != 0
}

func readTeamsMathFontAt(reader io.ReaderAt, data []byte, offset int64, size int64) error {
	if offset < 0 || int64(len(data)) > size-offset {
		return fmt.Errorf("font read is out of bounds")
	}
	read, err := reader.ReadAt(data, offset)
	if err != nil && !errorsIsEOF(err) {
		return fmt.Errorf("read font: %w", err)
	}
	if read != len(data) {
		return fmt.Errorf("font data is truncated")
	}
	return nil
}

func errorsIsEOF(err error) bool {
	return err == io.EOF
}
