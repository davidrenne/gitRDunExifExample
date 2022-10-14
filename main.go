package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	baseLog "log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/DanielRenne/GoCore/core/extensions"
	"github.com/DanielRenne/GoCore/core/logger"
	"github.com/DanielRenne/GoCore/core/path"
	"github.com/DanielRenne/GoCore/core/utils"
	"github.com/dsoprea/go-exif/v3"
	exifcommon "github.com/dsoprea/go-exif/v3/common"
	"github.com/dsoprea/go-iptc"
	jpegstructure "github.com/dsoprea/go-jpeg-image-structure/v2"
	log "github.com/dsoprea/go-logging/v2"
)

// Add your photo/video dump here
var folders = []string{
	"Z:" + path.PathSeparator + "Google Drive" + path.PathSeparator + "Family Photos",
}

type ReactComponentSync struct {
	sync.Mutex
	Items []string
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func RecurseFiles(fileDir string) (files []string, err error) {

	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()

	var wg sync.WaitGroup
	var syncedItems ReactComponentSync
	path := fileDir

	if extensions.DoesFileExist(path) == false {
		return
	}

	err = filepath.Walk(path, func(path string, f os.FileInfo, errWalk error) (err error) {

		if errWalk != nil {
			err = errWalk
			return
		}

		var readFile bool

		if !f.IsDir() {
			readFile = true
		}

		if readFile {
			wg.Add(1)

			go func() {
				defer wg.Done()
				syncedItems.Lock()
				syncedItems.Items = append(syncedItems.Items, path)
				syncedItems.Unlock()
			}()
		}
		return
	})
	wg.Wait()
	files = syncedItems.Items

	return
}

type processJob struct {
	Func func(string)
	File string
	Wg   *sync.WaitGroup
}

var (
	startJobLock      sync.Mutex
	lock              sync.RWMutex
	lockExifIPTCFull  sync.Mutex
	lockAllTimings    sync.Mutex
	lockAllUniqueTags sync.RWMutex
	lockExif          sync.Mutex
	lockExifCount     sync.Mutex
	once              sync.Once
	jobs              chan processJob
	foldersProcessed  []string
	jsonData          map[string][]exif.ExifTag
	jsonDataJPG       map[string]map[string]string
	jsonTimings       map[string][]string
	jsonUniqueNames   map[string][]uniqueNames
	totalFiles        int
)

func init() {
	numOfConcurrentUploads := 20
	jobs = make(chan processJob)

	jsonDataJPG = make(map[string]map[string]string, 0)
	jsonTimings = make(map[string][]string, 0)
	jsonUniqueNames = make(map[string][]uniqueNames, 0)
	totalFiles = 0

	data, err := extensions.ReadFile("exifCount.txt")
	if err == nil {
		totalFiles = extensions.StringToInt(string(data))
	}
	extensions.ReadFileAndParse("exifIPTCFull.json", &jsonDataJPG)
	extensions.ReadFileAndParse("allTimings.json", &jsonTimings)
	extensions.ReadFileAndParse("allUniqueTags.json", &jsonUniqueNames)

	for i := 0; i < numOfConcurrentUploads; i++ {
		go worker(i)
	}
}

func worker(idx int) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()

	for job := range jobs {
		job.Func(job.File)
		job.Wg.Done()
	}
}

const (
	thumbnailFilenameIndexPlaceholder = "<index>"
)

var (
	mainLogger = log.NewLogger("main.main")
)

var (
	filepathArg     = ""
	printAsJsonArg  = true
	printLoggingArg = false
)

// IfdEntry is a JSON model for representing a single tag.
type IfdEntry struct {
	IfdPath     string                      `json:"ifd_path"`
	FqIfdPath   string                      `json:"fq_ifd_path"`
	IfdIndex    int                         `json:"ifd_index"`
	TagId       uint16                      `json:"tag_id"`
	TagName     string                      `json:"tag_name"`
	TagTypeId   exifcommon.TagTypePrimitive `json:"tag_type_id"`
	TagTypeName string                      `json:"tag_type_name"`
	UnitCount   uint32                      `json:"unit_count"`
	Value       interface{}                 `json:"value"`
	ValueString string                      `json:"value_string"`
}

type segmentResult struct {
	MarkerId   byte           `json:"marker_id"`
	MarkerName string         `json:"marker_name"`
	Offset     int            `json:"offset"`
	FlatExif   []exif.ExifTag `json:"exif"`
	Length     int            `json:"length"`
}

type segmentIndexItem struct {
	Offset int    `json:"offset"`
	Data   []byte `json:"data"`
	Length int    `json:"length"`
}

type uniqueNames struct {
	Name       string      `json:"name"`
	SampleData interface{} `json:"sampleData"`
}

func main() {
	pictureExtensions := []string{
		"JPG", "TIF", "BMP", "PNG", "JPEG", "GIF", "CR2", "ARW", "HEIC", "NEF",
	}
	var wg sync.WaitGroup
	startEntireProcess := time.Now()
	var processJobs []processJob
	for _, folder := range folders {
		files, _ := RecurseFiles(folder)
		for _, fileToWorkOn := range files {
			pieces := strings.Split(fileToWorkOn, ".")
			ext := strings.ToUpper(pieces[len(pieces)-1:][0])
			if utils.InArray(ext, pictureExtensions) {
				processJobs = append(processJobs, processJob{
					Wg:   &wg,
					File: fileToWorkOn,
					Func: func(fileWork string) {
						pieces := strings.Split(fileWork, ".")
						ext := strings.ToUpper(pieces[len(pieces)-1:][0])
						pathPartial := strings.ReplaceAll(fileWork, folder+path.PathSeparator, "")
						piecesPath := strings.Split(pathPartial, path.PathSeparator)
						baseDir := piecesPath[0]
						if extensions.DoesFileExist("outIPTCJpg" + path.PathSeparator + "exifIPTCJpg-" + strings.ReplaceAll(pathPartial, path.PathSeparator, "-") + ".json") {
							return
						}
						if extensions.DoesFileExist("out" + path.PathSeparator + "exif-" + strings.ReplaceAll(pathPartial, path.PathSeparator, "-") + ".json") {
							return
						}
						baseLog.Println("Working on: " + fileWork)
						f, err := os.Open(fileWork)
						log.PanicIf(err)
						dataPicture, err := ioutil.ReadAll(f)
						log.PanicIf(err)
						fn := piecesPath[1]

						// JPG parsing
						if ext == "JPG" || ext == "JPEG" {
							start := time.Now()
							jmp := jpegstructure.NewJpegMediaParser()
							intfc, parseErr := jmp.ParseBytes(dataPicture)
							// If there was an error *and* we got back some segments, print the segments
							// before panicing.
							if intfc == nil && parseErr != nil {
								logger.Log("If there was an error *and* we got back some segments, print the segments", fileWork, parseErr.Error())
								return
							}
							sl := intfc.(*jpegstructure.SegmentList)

							startIptcFull := time.Now()
							tags, err := sl.Iptc()
							if err != nil {
								// ok here I need more test data on IPTC...??
								// logger.Log(" File has no Iptc data ", fileWork, err.Error())
							} else {
								distilled := iptc.GetDictionaryFromParsedTags(tags)
								sorted := jpegstructure.SortStringStringMap(distilled)
								lock.Lock()
								jsonDataJPG[baseDir+path.PathSeparator+fn] = make(map[string]string, 0)
								for _, pair := range sorted {
									jsonDataJPG[baseDir+path.PathSeparator+fn][pair[0]] = pair[1]
								}
								lock.Unlock()

								lockAllTimings.Lock()
								jsonTimings[baseDir+path.PathSeparator+fn] = append(jsonTimings[baseDir+path.PathSeparator+fn], logger.TimeTrack(startIptcFull, "iptc-full-"+baseDir+path.PathSeparator+fn))
								data, err := json.MarshalIndent(jsonTimings, "", "    ")
								if err == nil {
									err = extensions.Write(string(data), "allTimings.json")
								}
								lockAllTimings.Unlock()

								lockExifIPTCFull.Lock()
								lock.RLock()
								data, err = json.MarshalIndent(jsonDataJPG, "", "    ")
								lock.RUnlock()
								if err == nil {
									err = extensions.Write(string(data), "exifIPTCFull.json")
								}
								lockExifIPTCFull.Unlock()
								log.PanicIf(err)
							}

							jsonDataSegments := make(map[string][]segmentResult, 0)
							segments := make([]segmentResult, 0)
							segmentIndex := make(map[string][]segmentIndexItem)

							for _, s := range sl.Segments() {
								var data []byte
								// For now no data
								data = s.Data
								exifTags, err := s.FlatExif()
								if err == nil && len(s.Data) > 0 {
									segments = append(segments, segmentResult{
										MarkerId:   s.MarkerId,
										MarkerName: s.MarkerName,
										Offset:     s.Offset,
										Length:     len(s.Data),
										FlatExif:   exifTags,
									})

									lockAllUniqueTags.Lock()
									for _, tag := range exifTags {
										_, ok := jsonUniqueNames[tag.TagName]
										if !ok {
											jsonUniqueNames[tag.TagName] = append(jsonUniqueNames[tag.TagName], uniqueNames{
												Name:       tag.TagName,
												SampleData: tag.Value,
											})
										}
									}
									lockAllUniqueTags.Unlock()

									sii := segmentIndexItem{
										Offset: s.Offset,
										Length: len(s.Data),
										Data:   data,
									}

									if grouped, found := segmentIndex[s.MarkerName]; found == true {
										segmentIndex[s.MarkerName] = append(grouped, sii)
									} else {
										segmentIndex[s.MarkerName] = []segmentIndexItem{sii}
									}
								}
							}

							if parseErr != nil {
								fmt.Printf("JPEG Segments (incomplete due to error):\n")
								fmt.Printf("\n")

								sl.Print()

								fmt.Printf("\n")

								logger.Log("JPEG Segments (incomplete due to error):\n", fileWork, parseErr.Error())
								return
							}
							lockAllTimings.Lock()
							jsonTimings[baseDir+path.PathSeparator+fn] = append(jsonTimings[baseDir+path.PathSeparator+fn], logger.TimeTrack(start, "image-structure-"+baseDir+path.PathSeparator+fn))
							data, err := json.MarshalIndent(jsonTimings, "", "    ")
							if err == nil {
								err = extensions.Write(string(data), "allTimings.json")
							}
							lockAllTimings.Unlock()
							log.PanicIf(err)

							lockAllUniqueTags.RLock()
							data, err = json.MarshalIndent(jsonUniqueNames, "", "    ")
							if err == nil {
								err = extensions.Write(string(data), "allUniqueTags.json")
							}
							lockAllUniqueTags.RUnlock()
							log.PanicIf(err)

							jsonDataSegments[pathPartial] = append(jsonDataSegments[pathPartial], segments...)
							data, err = json.MarshalIndent(jsonDataSegments, "", "    ")
							if err == nil {
								extensions.MkDir("outIPTCJpg")
								extensions.Write(string(data), "outIPTCJpg"+path.PathSeparator+"exifIPTCJpg-"+strings.ReplaceAll(pathPartial, path.PathSeparator, "-")+".json")
							}
						}

						start := time.Now()
						rawExif, err := exif.SearchAndExtractExifN(dataPicture, 0)
						if err != nil {
							if err == exif.ErrNoExif {
								//logger.Log("No EXIF data.", fileWork)
								return
							}
							logger.Log("Error reading EXIF data from file", fileWork, err.Error())
							return
						}

						entries, _, err := exif.GetFlatExifDataUniversalSearch(rawExif, nil, true)
						if err != nil {
							logger.Log("GetFlatExifDataUniversalSearch", fileWork, err.Error())
						}

						lockAllTimings.Lock()
						jsonTimings[baseDir+path.PathSeparator+fn] = append(jsonTimings[baseDir+path.PathSeparator+fn], logger.TimeTrack(start, "exif-"+baseDir+path.PathSeparator+fn))
						data, err := json.MarshalIndent(jsonTimings, "", "    ")
						if err == nil {
							err = extensions.Write(string(data), "allTimings.json")
						}
						lockAllTimings.Unlock()
						log.PanicIf(err)

						lock.Lock()
						if !utils.InArray(baseDir, foldersProcessed) {
							foldersProcessed = append(foldersProcessed, baseDir)
							data, err = json.MarshalIndent(foldersProcessed, "", "    ")
							err = extensions.Write(string(data), "foldersProcessed.json")
						}
						totalFiles++
						lock.Unlock()

						jsonData = make(map[string][]exif.ExifTag, 0)
						jsonData[pathPartial] = append(jsonData[pathPartial], entries...)
						data, err = json.MarshalIndent(jsonData, "", "    ")
						if err == nil {
							extensions.MkDir("out")
							err = extensions.Write(string(data), "out"+path.PathSeparator+"exif-"+strings.ReplaceAll(pathPartial, path.PathSeparator, "-")+".json")
						}

						lockExif.Lock()
						lock.RLock()
						err = extensions.Write(extensions.IntToString(totalFiles), "exifCount.txt")
						lock.RUnlock()
						lockExif.Unlock()
						log.PanicIf(err)
					},
				})
			}
		}
	}

	wg.Add(len(processJobs))
	go func() {
		for _, job := range processJobs {
			j := job
			jobs <- j
		}
	}()

	logger.Log("Waiting on threads to finish...")
	wg.Wait()
	baseLog.Println(logger.TimeTrack(startEntireProcess, "Completed in"))
}
