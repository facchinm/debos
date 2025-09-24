/*
ImageStandalonePartitions Action

This action creates many image files, one per partition, and formats the filesystems.
Mountpoints can be defined so the created partitions can be mounted during the
build, and optionally (but by-default) mounted at boot in the final system. The
mountpoints are sorted on their position in the filesystem hierarchy so the
order in the recipe does not matter.

	# Yaml syntax:
	- action: image-standalone-partitions
	  imagebasename: image_base_name
	  partitions:
	    <list of partitions>
	  mountpoints:
	    <list of mount points>

Mandatory properties:

- imagename -- the name of the image file, relative to the artifact directory.

- imagesize -- generated image size in human-readable form, examples: 100MB, 1GB, etc.

- partitiontype -- partition table type. Currently only 'gpt' and 'msdos'
partition tables are supported.

- gpt_gap -- shifting GPT allow to use this gap for bootloaders, for example if
U-Boot intersects with original GPT placement.
Only works if parted supports an extra argument to mklabel to specify the gpt offset.

- partitions -- list of partitions, at least one partition is needed.
Partition properties are described below.

- mountpoints -- list of mount points for partitions.
Properties for mount points are described below.

Optional properties:

- diskid -- disk unique identifier string. For 'gpt' partition table, 'diskid'
should be in GUID format (e.g.: '00002222-4444-6666-AAAA-BBBBCCCCFFFF' where each
character is an hexadecimal digit). For 'msdos' partition table, 'diskid' should be
a 32 bits hexadecimal number (e.g. '1234ABCD' without any dash separator).

	   # Yaml syntax for partitions:
	   partitions:
	     - name: partition name
		   partlabel: partition label
		   fs: filesystem
		   fslabel: filesystem label
		   size: size
		   features: list of filesystem features
		   extendedoptions: list of filesystem extended options
		   flags: list of flags
		   fsck: bool
		   fsuuid: string
		   partuuid: string
		   partattrs: list of partition attribute bits to set

Mandatory properties:

- name -- is used for referencing named partition for mount points
configuration (below) and label the filesystem located on this partition. Must be
unique.

- fs -- filesystem type used for formatting.

'none' fs type should be used for partition without filesystem.

- size -- partition size.

The size can be specified in human-readable form, examples: 100MB, 1GB, etc.

Optional properties:

- partlabel -- label for the partition in the GPT partition table. Defaults
to the `name` property of the partition. May only be used for GPT partitions.

- fslabel -- label for the filesystem. Defaults
to the `name` property of the partition. The filesystem label can be up to 11
characters long for {v}fat{12|16|32}, 16 characters long for ext2/3/4, 255
characters long for btrfs, 512 characters long for hfs/hfsplus and 12 characters
long for xfs.

- parttype -- set the partition type in the partition table. The string should
be in a hexadecimal format (2-characters) for msdos partition tables and GUID format
(36-characters) for GPT partition tables. For instance, "82" for msdos sets the
partition type to Linux Swap. Whereas "0657fd6d-a4ab-43c4-84e5-0933c84b4f4f" for
GPT sets the partition type to Linux Swap.
For msdos partition types hex codes see: https://en.wikipedia.org/wiki/Partition_type
For gpt partition type GUIDs see: https://systemd.io/DISCOVERABLE_PARTITIONS/

- features -- list of additional filesystem features which need to be enabled
for partition.

- flags -- list of additional flags for partition compatible with parted(8)
'set' command.

- partattrs -- list of GPT partition attribute bits to set, as defined in
https://uefi.org/specs/UEFI/2.10/05_GUID_Partition_Table_Format.html#defined-gpt-partition-entry-attributes.
Bit 0: "Required Partition", bit 1: "No Block IO Protocol", bit 2: "Legacy BIOS
Bootable". Bits 3-47 are reserved. Bits 48-63 are GUID specific. For example,
ChromeOS Kernel partitions (GUID=fe3a2a5d-4f32-41a7-b725-accc3285a309) use bit
56 for "successful boot" and bits 48-51 for "priority", where 0 means not
bootable, thus bits 56 and 48 need to be set through this property in order to
be able to boot a ChromeOS Kernel partition on a Chromebook, like so:
'partattrs: [56, 48]'.

- fsck -- if set to `false` -- then set fs_passno (man fstab) to 0 meaning no filesystem
checks in boot time. By default is set to `true` allowing checks on boot.

- fsuuid -- file system UUID string. This option is only supported for btrfs,
ext2, ext3, ext4 and xfs.

- partuuid -- GPT partition UUID string.
A version 5 UUID can be easily generated using the uuid5 template function
{{ uuid5 $namespace $data }} $namespace should be a valid UUID and $data can be
any string, to generate reproducible UUID value pass a fixed value of namespace
and data.

- extendedoptions -- list of additional filesystem extended options which need
to be enabled for the partition.

	   # Yaml syntax for mount points:
	   mountpoints:
	     - mountpoint: path
		   partition: partition label
		   options: list of options
		   buildtime: bool

Mandatory properties:

- partition -- partition name for mounting. The partion must exist under `partitions`.

- mountpoint -- path in the target root filesystem where the named partition
should be mounted. Must be unique, only one partition can be mounted per
mountpoint.

Optional properties:

- options -- list of options to be added to appropriate entry in fstab file.

- buildtime -- if set to true then the mountpoint only used during the debos run.
No entry in `/etc/fstab` will be created.
The mountpoints directory will be removed from the image, so it is recommended
to define a `mountpoint` path which is temporary and unique for the image,
for example: `/mnt/temporary_mount`.
Defaults to false.

	# Layout example for Raspberry PI 3:
	- action: image-partition
	  imagename: "debian-rpi3.img"
	  imagesize: 1GB
	  partitiontype: msdos
	  mountpoints:
	    - mountpoint: /
	      partition: root
	    - mountpoint: /boot/firmware
	      partition: firmware
	      options: [ x-systemd.automount ]
	  partitions:
	    - name: firmware
	      fs: vfat
	      start: 0%
	      end: 64MB
	    - name: root
	      fs: ext4
	      start: 64MB
	      end: 100%
	      flags: [ boot ]
*/
package actions

import (
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/docker/go-units"
	"github.com/go-debos/fakemachine"
	"github.com/google/uuid"
	"log"
	"os"
	"os/exec"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/go-debos/debos"
)

type StandalonePartition struct {
	number          int
	realsize        int64
	Name            string
	PartLabel       string
	FSLabel         string
	PartType        string
	PartAttrs       []string
	PartUUID        string
	Size            string
	Path            string
	FS              string
	Flags           []string
	Features        []string
	ExtendedOptions []string
	Fsck            bool `yaml:"fsck"`
	FSUUID          string
	ResizeAtBoot    bool `yaml:"resizeatboot"`
}

type StandaloneMountpoint struct {
	Mountpoint string
	Partition  string
	Options    []string
	Buildtime  bool
	part       *StandalonePartition
}

type ImageStandalonePartitionsAction struct {
	debos.BaseAction `yaml:",inline"`
	ImageBaseName    string
	ImageSize        string
	PartitionType    string
	DiskID           string
	GptGap           string `yaml:"gpt_gap"`
	Partitions       []StandalonePartition
	Mountpoints      []StandaloneMountpoint
}

func (p *StandalonePartition) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type rawPartition StandalonePartition
	part := rawPartition{Fsck: true}
	if err := unmarshal(&part); err != nil {
		return err
	}
	*p = StandalonePartition(part)
	return nil
}

func (i *ImageStandalonePartitionsAction) generateFSTab(context *debos.DebosContext) error {
	context.ImageFSTab.Reset()

	for _, m := range i.Mountpoints {
		options := []string{"defaults"}
		options = append(options, m.Options...)
		if m.Buildtime {
			/* Do not need to add mount point into fstab */
			continue
		}
		if m.part.FSUUID == "" {
			return fmt.Errorf("missing fs UUID for partition %s", m.part.Name)
		}

		fs_passno := 0

		if m.part.Fsck {
			if m.Mountpoint == "/" {
				fs_passno = 1
			} else {
				fs_passno = 2
			}
		}

		fsType := m.part.FS
		switch m.part.FS {
		case "fat", "fat12", "fat16", "fat32", "msdos":
			fsType = "vfat"
		}

		context.ImageFSTab.WriteString(fmt.Sprintf("UUID=%s\t%s\t%s\t%s\t0\t%d\n",
			m.part.FSUUID, m.Mountpoint, fsType,
			strings.Join(options, ","), fs_passno))
	}

	return nil
}

func (i *ImageStandalonePartitionsAction) generateKernelRoot(context *debos.DebosContext) error {
	for _, m := range i.Mountpoints {
		if m.Mountpoint == "/" {
			if m.part.FSUUID == "" {
				return errors.New("no fs UUID for root partition")
			}
			context.ImageKernelRoot = fmt.Sprintf("root=UUID=%s", m.part.FSUUID)
			break
		}
	}

	return nil
}

func (i ImageStandalonePartitionsAction) PreMachine(context *debos.DebosContext, m *fakemachine.Machine,
	args *[]string) error {
	return nil
}

func (i ImageStandalonePartitionsAction) formatPartition(p *StandalonePartition, context debos.DebosContext) error {
	label := fmt.Sprintf("Formatting partition %d", p.number)
	path := p.Path

	cmdline := []string{}
	switch p.FS {
	case "fat", "fat12", "fat16", "fat32", "msdos", "vfat":
		cmdline = append(cmdline, "mkfs.vfat", "-n", p.FSLabel)

		switch p.FS {
		case "fat12":
			cmdline = append(cmdline, "-F12")
		case "fat16":
			cmdline = append(cmdline, "-F16")
		case "fat32", "msdos", "vfat":
			cmdline = append(cmdline, "-F32")
		default:
			/* let mkfs.vfat autodetermine FAT type */
			break
		}

		if len(p.FSUUID) > 0 {
			cmdline = append(cmdline, "-i", p.FSUUID)
		}
	case "btrfs":
		// Force formatting to prevent failure in case if partition was formatted already
		cmdline = append(cmdline, "mkfs.btrfs", "-L", p.FSLabel, "-f")
		if len(p.Features) > 0 {
			cmdline = append(cmdline, "-O", strings.Join(p.Features, ","))
		}
		if len(p.FSUUID) > 0 {
			cmdline = append(cmdline, "-U", p.FSUUID)
		}
	case "f2fs":
		cmdline = append(cmdline, "mkfs.f2fs", "-l", p.FSLabel)
		if len(p.Features) > 0 {
			cmdline = append(cmdline, "-O", strings.Join(p.Features, ","))
		}
	case "hfs":
		cmdline = append(cmdline, "mkfs.hfs", "-h", "-v", p.FSLabel)
	case "hfsplus":
		cmdline = append(cmdline, "mkfs.hfsplus", "-v", p.FSLabel)
	case "hfsx":
		cmdline = append(cmdline, "mkfs.hfsplus", "-s", "-v", p.FSLabel)
		// hfsx is case-insensitive hfs+, should be treated as "normal" hfs+ from now on
		p.FS = "hfsplus"
	case "xfs":
		cmdline = append(cmdline, "mkfs.xfs", "-L", p.FSLabel)
		if len(p.FSUUID) > 0 {
			cmdline = append(cmdline, "-m", "uuid="+p.FSUUID)
		}
	case "none":
	default:
		cmdline = append(cmdline, fmt.Sprintf("mkfs.%s", p.FS), "-L", p.FSLabel)
		if len(p.Features) > 0 {
			cmdline = append(cmdline, "-O", strings.Join(p.Features, ","))
		}
		if len(p.ExtendedOptions) > 0 {
			cmdline = append(cmdline, "-E", strings.Join(p.ExtendedOptions, ","))
		}
		if len(p.FSUUID) > 0 {
			if p.FS == "ext2" || p.FS == "ext3" || p.FS == "ext4" {
				cmdline = append(cmdline, "-U", p.FSUUID)
			}
		}
	}

	if len(cmdline) != 0 {
		cmdline = append(cmdline, path)

		cmd := debos.Command{}

		/* Some underlying device driver, e.g. the UML UBD driver, may manage holes
		 * incorrectly which will prevent to retrieve all useful zero ranges in
		 * filesystem, e.g. when using 'bmaptool create', see patch
		 * http://lists.infradead.org/pipermail/linux-um/2022-January/002074.html
		 *
		 * Adding UNIX_IO_NOZEROOUT environment variable prevent mkfs.ext[234]
		 * utilities to create zero range spaces using fallocate with
		 * FALLOC_FL_ZERO_RANGE or FALLOC_FL_PUNCH_HOLE */
		if p.FS == "ext2" || p.FS == "ext3" || p.FS == "ext4" {
			cmd.AddEnv("UNIX_IO_NOZEROOUT=1")
		}

		if err := cmd.Run(label, cmdline...); err != nil {
			return err
		}
	}

	if p.FS != "none" && p.FSUUID == "" {
		uuid, err := exec.Command("blkid", "-o", "value", "-s", "UUID", "-p", "-c", "none", path).Output()
		if err != nil {
			return fmt.Errorf("failed to get uuid: %s", err)
		}
		p.FSUUID = strings.TrimSpace(string(uuid[:]))
	}

	return nil
}

func (i *ImageStandalonePartitionsAction) PreNoMachine(context *debos.DebosContext) error {
	return nil
}

func (i ImageStandalonePartitionsAction) Run(context *debos.DebosContext) error {

	for idx := range i.Partitions {

		imagePath := path.Join(context.Artifactdir, i.ImageBaseName+"."+i.Partitions[idx].Name)
		img, err := os.OpenFile(imagePath, os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			return fmt.Errorf("couldn't open image file: %v", err)
		}

		err = img.Truncate(i.Partitions[idx].realsize)
		if err != nil {
			return fmt.Errorf("couldn't resize image file: %v", err)
		}

		img.Close()
		i.Partitions[idx].Path = imagePath

		err = i.formatPartition(&i.Partitions[idx], *context)
		if err != nil {
			return err
		}

		devicePath := imagePath
		context.ImagePartitions = append(context.ImagePartitions,
			debos.Partition{Name: i.Partitions[idx].Name, DevicePath: devicePath})
	}

	context.ImageMntDir = path.Join(context.Scratchdir, "mnt")
	os.MkdirAll(context.ImageMntDir, 0755)

	// sort mountpoints based on position in filesystem hierarchy
	sort.SliceStable(i.Mountpoints, func(a, b int) bool {
		mntA := i.Mountpoints[a].Mountpoint
		mntB := i.Mountpoints[b].Mountpoint

		// root should always be mounted first
		if mntA == "/" {
			return true
		}
		if mntB == "/" {
			return false
		}

		return strings.Count(mntA, "/") < strings.Count(mntB, "/")
	})

	for _, m := range i.Mountpoints {
		dev := m.part.Path
		mntpath := path.Join(context.ImageMntDir, m.Mountpoint)
		os.MkdirAll(mntpath, 0755)
		out, err := exec.Command("mount", dev, mntpath).Output()
		if err != nil {
			return fmt.Errorf("%s mount failed: tried mounting %s on %s: %v", m.part.Name, dev, mntpath, err)
		} else {
			fmt.Println(out)
		}
	}

	err := i.generateFSTab(context)
	if err != nil {
		return err
	}

	err = i.generateKernelRoot(context)
	if err != nil {
		return err
	}

	return nil
}

func (i ImageStandalonePartitionsAction) Cleanup(context *debos.DebosContext) error {
	for idx := len(i.Mountpoints) - 1; idx >= 0; idx-- {
		m := i.Mountpoints[idx]
		mntpath := path.Join(context.ImageMntDir, m.Mountpoint)
		_, err := exec.Command("umount", mntpath).Output()
		if err != nil {
			log.Printf("Warning: Failed to get unmount %s: %s", m.Mountpoint, err)
			log.Printf("Unmount failure can cause images being incomplete!")
			return err
		}
		if m.Buildtime {
			if err = os.Remove(mntpath); err != nil {
				log.Printf("Failed to remove temporary mount point %s: %s", m.Mountpoint, err)

				if err.(*os.PathError).Err.Error() == "read-only file system" {
					continue
				}

				return err
			}
		}
	}

	return nil
}

func (i ImageStandalonePartitionsAction) PostMachineCleanup(context *debos.DebosContext) error {
	/* Remove the images in case of any action failure */
	if context.State != debos.Success {
		for partition := range context.ImagePartitions {
			image := path.Join(context.Artifactdir, context.ImagePartitions[partition].Name)
			if _, err := os.Stat(image); !os.IsNotExist(err) {
				if err = os.Remove(image); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (i *ImageStandalonePartitionsAction) Verify(context *debos.DebosContext) error {
	if i.PartitionType == "msdos" {
		for idx := range i.Partitions {
			p := &i.Partitions[idx]

			if idx == 3 && len(i.Partitions) > 4 {
				var name string
				var part StandalonePartition

				name = "extended"
				part.number = idx + 1
				part.Name = name
				part.Size = p.Size
				part.FS = "none"

				i.Partitions = append(i.Partitions[:idx+1], i.Partitions[idx:]...)
				i.Partitions[idx] = part

				num := 1
				for idx := range i.Partitions {
					p := &i.Partitions[idx]
					p.number = num
					num++
				}
			}
		}
	}

	if len(i.GptGap) > 0 {
		log.Println("WARNING: special version of parted is needed for 'gpt_gap' option")
		if i.PartitionType != "gpt" {
			return fmt.Errorf("gpt_gap property could be used only with 'gpt' label")
		}
		// Just check if it contains correct value
		_, err := units.FromHumanSize(i.GptGap)
		if err != nil {
			return fmt.Errorf("failed to parse image size: %s", i.GptGap)
		}
	}

	if len(i.DiskID) > 0 {
		switch i.PartitionType {
		case "gpt":
			_, err := uuid.Parse(i.DiskID)
			if err != nil {
				return fmt.Errorf("incorrect disk GUID %s", i.DiskID)
			}
		case "msdos":
			_, err := hex.DecodeString(i.DiskID)
			if err != nil || len(i.DiskID) != 8 {
				return fmt.Errorf("incorrect disk ID %s, should be 32-bit hexadecimal number", i.DiskID)
			}
			// Add 0x prefix
			i.DiskID = "0x" + i.DiskID
		}
	}

	num := 1
	for idx := range i.Partitions {
		var maxLength = 0
		p := &i.Partitions[idx]
		p.number = num
		num++
		if p.Name == "" {
			return fmt.Errorf("partition without a name")
		}

		// check for duplicate partition names
		for j := idx + 1; j < len(i.Partitions); j++ {
			if i.Partitions[j].Name == p.Name {
				return fmt.Errorf("partition %s already exists", p.Name)
			}
		}

		if len(p.FSUUID) > 0 {
			switch p.FS {
			case "btrfs", "ext2", "ext3", "ext4", "xfs":
				_, err := uuid.Parse(p.FSUUID)
				if err != nil {
					return fmt.Errorf("incorrect UUID %s", p.FSUUID)
				}
			case "fat", "fat12", "fat16", "fat32", "msdos", "vfat":
				_, err := hex.DecodeString(p.FSUUID)
				if err != nil || len(p.FSUUID) != 8 {
					return fmt.Errorf("incorrect UUID %s, should be 32-bit hexadecimal number", p.FSUUID)
				}
			default:
				return fmt.Errorf("setting the UUID is not supported for filesystem %s", p.FS)
			}
		}

		if len(p.PartUUID) > 0 {
			switch i.PartitionType {
			case "gpt":
				_, err := uuid.Parse(p.PartUUID)
				if err != nil {
					return fmt.Errorf("incorrect partition UUID %s", p.PartUUID)
				}
			default:
				return fmt.Errorf("setting the partition UUID is not supported for %s", i.PartitionType)
			}
		}

		for _, bitStr := range p.PartAttrs {
			bit, err := strconv.ParseInt(bitStr, 0, 0)
			if err != nil || bit < 0 || bit > 2 && bit < 48 || bit > 63 {
				return fmt.Errorf("partition attribute bit '%s' outside of valid range (0-2, 48-63)", bitStr)
			}
		}

		if p.Size == "" {
			return fmt.Errorf("partition %s missing size", p.Name)
		}

		if p.FS == "" {
			return fmt.Errorf("partition %s missing fs type", p.Name)
		}

		if p.FSLabel == "" {
			p.FSLabel = p.Name
		}

		switch p.FS {
		case "fat", "fat12", "fat16", "fat32", "msdos", "vfat":
			maxLength = 11
		case "ext2", "ext3", "ext4":
			maxLength = 16
		case "btrfs":
			maxLength = 255
		case "f2fs":
			maxLength = 512
		case "hfs", "hfsplus":
			maxLength = 255
		case "xfs":
			maxLength = 12
		case "none":
		default:
			log.Printf("Warning: setting a fs label for %s is unsupported", p.FS)
		}

		if maxLength > 0 && len(p.FSLabel) > maxLength {
			return fmt.Errorf("fs label for %s '%s' is too long", p.Name, p.FSLabel)
		}

		// Calculate the size based on the unit (binary or decimal)
		// binary units are multiples of 1024 - KiB, MiB, GiB, TiB, PiB
		// decimal units are multiples of 1000 - KB, MB, GB, TB, PB
		var getSizeValueFunc func(size string) (int64, error)
		if regexp.MustCompile(`^[0-9.]+[kmgtp]ib+$`).MatchString(strings.ToLower(i.ImageSize)) {
			getSizeValueFunc = units.RAMInBytes
		} else {
			getSizeValueFunc = units.FromHumanSize
		}

		size, err := getSizeValueFunc(p.Size)
		if err != nil {
			return fmt.Errorf("failed to parse image size: %s", i.ImageSize)
		}

		p.realsize = size
	}

	for idx := range i.Mountpoints {
		m := &i.Mountpoints[idx]

		// check for duplicate mountpoints
		for j := idx + 1; j < len(i.Mountpoints); j++ {
			if i.Mountpoints[j].Mountpoint == m.Mountpoint {
				return fmt.Errorf("mountpoint %s already exists", m.Mountpoint)
			}
		}

		for pidx := range i.Partitions {
			p := &i.Partitions[pidx]
			if m.Partition == p.Name {
				m.part = p
				break
			}
		}
		if m.part == nil {
			return fmt.Errorf("couldn't find partition for %s", m.Mountpoint)
		}

		if strings.ToLower(m.part.FS) == "none" {
			return fmt.Errorf("cannot mount %s: filesystem not present", m.Mountpoint)
		}
	}

	return nil
}
