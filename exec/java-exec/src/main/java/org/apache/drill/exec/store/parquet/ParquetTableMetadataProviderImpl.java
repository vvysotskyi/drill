/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.parquet;

import org.apache.drill.exec.physical.base.ParquetTableMetadataProvider;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.MetadataContext;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.exec.store.parquet.metadata.Metadata;
import org.apache.drill.exec.store.parquet.metadata.MetadataBase;
import org.apache.drill.exec.util.DrillFileSystemUtil;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ParquetTableMetadataProviderImpl extends BaseParquetMetadataProvider implements ParquetTableMetadataProvider {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetTableMetadataProviderImpl.class);

  private final DrillFileSystem fs;
  private final MetadataContext metaContext;
  // may change when filter push down / partition pruning is applied
  private String selectionRoot;
  private String cacheFileRoot;
  private final boolean corruptDatesAutoCorrected;
  private boolean usedMetadataCache; // false by default

  public ParquetTableMetadataProviderImpl(List<ReadEntryWithPath> entries,
                                          String selectionRoot,
                                          String cacheFileRoot,
                                          ParquetReaderConfig readerConfig,
                                          DrillFileSystem fs,
                                          boolean autoCorrectCorruptedDates) throws IOException {
    super(entries, readerConfig, selectionRoot, selectionRoot);
    this.fs = fs;
    this.selectionRoot = selectionRoot;
    this.cacheFileRoot = cacheFileRoot;
    this.metaContext = new MetadataContext();

    this.corruptDatesAutoCorrected = autoCorrectCorruptedDates;

    init();
  }

  public ParquetTableMetadataProviderImpl(FileSelection selection,
                                          ParquetReaderConfig readerConfig,
                                          DrillFileSystem fs,
                                          boolean autoCorrectCorruptedDates) throws IOException {
    super(readerConfig, new ArrayList<>(), selection.getSelectionRoot(), selection.getSelectionRoot());

    this.fs = fs;
    this.selectionRoot = selection.getSelectionRoot();
    this.cacheFileRoot = selection.getCacheFileRoot();

    MetadataContext metadataContext = selection.getMetaContext();
    this.metaContext = metadataContext != null ? metadataContext : new MetadataContext();

    this.corruptDatesAutoCorrected = autoCorrectCorruptedDates;

    FileSelection fileSelection = expandIfNecessary(selection);
    if (fileSelection != null) {
      if (checkForInitializingEntriesWithSelectionRoot()) {
        // The fully expanded list is already stored as part of the fileSet
        entries.add(new ReadEntryWithPath(fileSelection.getSelectionRoot()));
      } else {
        for (String fileName : fileSelection.getFiles()) {
          entries.add(new ReadEntryWithPath(fileName));
        }
      }

      init();
    }
  }

  @Override
  public boolean isUsedMetadataCache() {
    return usedMetadataCache;
  }

  @Override
  public String getSelectionRoot() {
    return selectionRoot;
  }

  @Override
  protected void initInternal() throws IOException {
    try (FileSystem processUserFileSystem = ImpersonationUtil.createFileSystem(ImpersonationUtil.getProcessUserName(), fs.getConf())) {
      Path metaPath = null;
      if (entries.size() == 1 && parquetTableMetadata == null) {
        Path p = Path.getPathWithoutSchemeAndAuthority(new Path(entries.get(0).getPath()));
        if (fs.isDirectory(p)) {
          // Using the metadata file makes sense when querying a directory; otherwise
          // if querying a single file we can look up the metadata directly from the file
          metaPath = new Path(p, Metadata.METADATA_FILENAME);
        }
        if (!metaContext.isMetadataCacheCorrupted() && metaPath != null && fs.exists(metaPath)) {
          parquetTableMetadata = Metadata.readBlockMeta(processUserFileSystem, metaPath, metaContext, readerConfig);
          if (parquetTableMetadata != null) {
            usedMetadataCache = true;
          }
        }
        if (!usedMetadataCache) {
          parquetTableMetadata = Metadata.getParquetTableMetadata(processUserFileSystem, p.toString(), readerConfig);
        }
      } else {
        Path p = Path.getPathWithoutSchemeAndAuthority(new Path(selectionRoot));
        metaPath = new Path(p, Metadata.METADATA_FILENAME);
        if (!metaContext.isMetadataCacheCorrupted() && fs.isDirectory(new Path(selectionRoot))
            && fs.exists(metaPath)) {
          if (parquetTableMetadata == null) {
            parquetTableMetadata = Metadata.readBlockMeta(processUserFileSystem, metaPath, metaContext, readerConfig);
          }
          if (parquetTableMetadata != null) {
            usedMetadataCache = true;
            if (fileSet != null) {
              parquetTableMetadata = removeUnneededRowGroups(parquetTableMetadata);
            }
          }
        }
        if (!usedMetadataCache) {
          final List<FileStatus> fileStatuses = new ArrayList<>();
          for (ReadEntryWithPath entry : entries) {
            fileStatuses.addAll(
                DrillFileSystemUtil.listFiles(fs, Path.getPathWithoutSchemeAndAuthority(new Path(entry.getPath())), true));
          }

          Map<FileStatus, FileSystem> statusMap = fileStatuses.stream()
              .collect(
                Collectors.toMap(
                  Function.identity(),
                  s -> processUserFileSystem,
                  (oldFs, newFs) -> newFs,
                  LinkedHashMap::new));

          parquetTableMetadata = Metadata.getParquetTableMetadata(statusMap, readerConfig);
        }
      }
    }
  }

  // private methods block start
  /**
   * Expands the selection's folders if metadata cache is found for the selection root.<br>
   * If the selection has already been expanded or no metadata cache was found, does nothing
   *
   * @param selection actual selection before expansion
   * @return new selection after expansion, if no expansion was done returns the input selection
   */
  private FileSelection expandIfNecessary(FileSelection selection) throws IOException {
    if (selection.isExpandedFully()) {
      return selection;
    }

    // use the cacheFileRoot if provided (e.g after partition pruning)
    Path metaFilePath = new Path(cacheFileRoot != null ? cacheFileRoot : selectionRoot, Metadata.METADATA_FILENAME);
    if (!fs.exists(metaFilePath)) { // no metadata cache
      if (selection.isExpandedPartial()) {
        logger.error("'{}' metadata file does not exist, but metadata directories cache file is present", metaFilePath);
        metaContext.setMetadataCacheCorrupted(true);
      }

      return selection;
    }

    return expandSelectionFromMetadataCache(selection, metaFilePath);
  }

  /**
   * For two cases the entries should be initialized with just the selection root instead of the fully expanded list:
   * <ul>
   *   <li> When metadata caching is corrupted (to use correct file selection)
   *   <li> Metadata caching is correct and used, but pruning was not applicable or was attempted and nothing was pruned
   *        (to reduce overhead in parquet group scan).
   * </ul>
   *
   * @return true if entries should be initialized with selection root, false otherwise
   */
  private boolean checkForInitializingEntriesWithSelectionRoot() {
    return metaContext.isMetadataCacheCorrupted() || (parquetTableMetadata != null &&
        (metaContext.getPruneStatus() == MetadataContext.PruneStatus.NOT_STARTED || metaContext.getPruneStatus() == MetadataContext.PruneStatus.NOT_PRUNED));
  }

  /**
   * Create and return a new file selection based on reading the metadata cache file.
   *
   * This function also initializes a few of ParquetGroupScan's fields as appropriate.
   *
   * @param selection initial file selection
   * @param metaFilePath metadata cache file path
   * @return file selection read from cache
   *
   * @throws org.apache.drill.common.exceptions.UserException when the updated selection is empty, this happens if the user selects an empty folder.
   */
  private FileSelection expandSelectionFromMetadataCache(FileSelection selection, Path metaFilePath) throws IOException {
    // get the metadata for the root directory by reading the metadata file
    // parquetTableMetadata contains the metadata for all files in the selection root folder, but we need to make sure
    // we only select the files that are part of selection (by setting fileSet appropriately)

    // get (and set internal field) the metadata for the directory by reading the metadata file
    FileSystem processUserFileSystem = ImpersonationUtil.createFileSystem(ImpersonationUtil.getProcessUserName(), fs.getConf());
    parquetTableMetadata = Metadata.readBlockMeta(processUserFileSystem, metaFilePath, metaContext, readerConfig);
    if (ignoreExpandingSelection(parquetTableMetadata)) {
      return selection;
    }
    if (corruptDatesAutoCorrected) {
      ParquetReaderUtility.correctDatesInMetadataCache(this.parquetTableMetadata);
    }
    ParquetReaderUtility.transformBinaryInMetadataCache(parquetTableMetadata, readerConfig);
    List<FileStatus> fileStatuses = selection.getStatuses(fs);

    if (fileSet == null) {
      fileSet = new HashSet<>();
    }

    final Path first = fileStatuses.get(0).getPath();
    if (fileStatuses.size() == 1 && selection.getSelectionRoot().equals(first.toString())) {
      // we are selecting all files from selection root. Expand the file list from the cache
      for (MetadataBase.ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
        fileSet.add(file.getPath());
      }

    } else if (selection.isExpandedPartial() && !selection.hadWildcard() && cacheFileRoot != null) {
      if (selection.wasAllPartitionsPruned()) {
        // if all partitions were previously pruned, we only need to read 1 file (for the schema)
        fileSet.add(this.parquetTableMetadata.getFiles().get(0).getPath());
      } else {
        // we are here if the selection is in the expanded_partial state (i.e it has directories).  We get the
        // list of files from the metadata cache file that is present in the cacheFileRoot directory and populate
        // the fileSet. However, this is *not* the final list of files that will be scanned in execution since the
        // second phase of partition pruning will apply on the files and modify the file selection appropriately.
        for (MetadataBase.ParquetFileMetadata file : this.parquetTableMetadata.getFiles()) {
          fileSet.add(file.getPath());
        }
      }
    } else {
      // we need to expand the files from fileStatuses
      for (FileStatus status : fileStatuses) {
        Path currentCacheFileRoot = status.getPath();
        if (status.isDirectory()) {
          //TODO [DRILL-4496] read the metadata cache files in parallel
          Path metaPath = new Path(currentCacheFileRoot, Metadata.METADATA_FILENAME);
          MetadataBase.ParquetTableMetadataBase metadata = Metadata.readBlockMeta(processUserFileSystem, metaPath, metaContext, readerConfig);
          if (ignoreExpandingSelection(metadata)) {
            return selection;
          }
          for (MetadataBase.ParquetFileMetadata file : metadata.getFiles()) {
            fileSet.add(file.getPath());
          }
        } else {
          final Path path = Path.getPathWithoutSchemeAndAuthority(currentCacheFileRoot);
          fileSet.add(path.toString());
        }
      }
    }

    if (fileSet.isEmpty()) {
      // no files were found, most likely we tried to query some empty sub folders
      logger.warn("The table is empty but with outdated invalid metadata cache files. Please, delete them.");
      return null;
    }

    List<String> fileNames = new ArrayList<>(fileSet);

    // when creating the file selection, set the selection root without the URI prefix
    // The reason is that the file names above have been created in the form
    // /a/b/c.parquet and the format of the selection root must match that of the file names
    // otherwise downstream operations such as partition pruning can break.
    Path metaRootPath = Path.getPathWithoutSchemeAndAuthority(new Path(selection.getSelectionRoot()));
    this.selectionRoot = metaRootPath.toString();

    // Use the FileSelection constructor directly here instead of the FileSelection.create() method
    // because create() changes the root to include the scheme and authority; In future, if create()
    // is the preferred way to instantiate a file selection, we may need to do something different...
    // WARNING: file statuses and file names are inconsistent
    FileSelection newSelection = new FileSelection(selection.getStatuses(fs), fileNames, metaRootPath.toString(),
        cacheFileRoot, selection.wasAllPartitionsPruned());

    newSelection.setExpandedFully();
    newSelection.setMetaContext(metaContext);
    return newSelection;
  }

  private MetadataBase.ParquetTableMetadataBase removeUnneededRowGroups(MetadataBase.ParquetTableMetadataBase parquetTableMetadata) {
    List<MetadataBase.ParquetFileMetadata> newFileMetadataList = new ArrayList<>();
    for (MetadataBase.ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
      if (fileSet.contains(file.getPath())) {
        newFileMetadataList.add(file);
      }
    }

    MetadataBase.ParquetTableMetadataBase metadata = parquetTableMetadata.clone();
    metadata.assignFiles(newFileMetadataList);
    return metadata;
  }

  /**
   * If metadata is corrupted, ignore expanding selection and reset parquetTableMetadata and fileSet fields
   *
   * @param metadata parquet table metadata
   * @return true if parquet metadata is corrupted, false otherwise
   */
  private boolean ignoreExpandingSelection(MetadataBase.ParquetTableMetadataBase metadata) {
    if (metadata == null || metaContext.isMetadataCacheCorrupted()) {
      logger.debug("Selection can't be expanded since metadata file is corrupted or metadata version is not supported");
      this.parquetTableMetadata = null;
      this.fileSet = null;
      return true;
    }
    return false;
  }
}
