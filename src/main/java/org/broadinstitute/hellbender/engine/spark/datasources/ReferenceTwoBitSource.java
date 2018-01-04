package org.broadinstitute.hellbender.engine.spark.datasources;

import com.google.common.io.ByteStreams;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import org.bdgenomics.adam.models.ReferenceRegion;
import org.bdgenomics.adam.models.SequenceDictionary;
import org.bdgenomics.adam.models.SequenceRecord;
import org.bdgenomics.adam.util.TwoBitFile;
import org.bdgenomics.formats.avro.Strand;
import org.bdgenomics.utils.io.ByteAccess;
import org.broadinstitute.hellbender.engine.datasources.ReferenceSource;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import org.broadinstitute.hellbender.utils.reference.ReferenceBases;
import scala.Option;
import scala.collection.JavaConversions;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A ReferenceSource impl that is backed by a .2bit representation of a reference genome.  This loads an entire .2bit
 * file into a byte array that is encapsulated by this object.  This is particularly useful for fast reference queries
 * if the entire reference can fit into memory.
 */
public class ReferenceTwoBitSource implements ReferenceSource, Serializable {
    private static final long serialVersionUID = 1L;

    public static final String TWO_BIT_EXTENSION = ".2bit";

    private final String referenceURL;
    private final TwoBitFile twoBitFile;
    private final SequenceDictionary twoBitSeqDictionary;

    public ReferenceTwoBitSource(String referenceURL) throws IOException {
        this.referenceURL = referenceURL;
        Utils.validateArg(isTwoBit(this.referenceURL), "ReferenceTwoBitSource can only take .2bit files");
        byte[] bytes = ByteStreams.toByteArray(BucketUtils.openFile(this.referenceURL));
        ByteAccess byteAccess = new DirectFullByteArrayByteAccess(bytes);
        this.twoBitFile = new TwoBitFile(byteAccess);
        this.twoBitSeqDictionary = twoBitFile.sequences();
    }

    /**
     * Gets the reference bases spanning the requested interval. If the interval ends beyond the end of its
     * contig according to our reference source's dictionary, it will be truncated at the contig end.
     *
     * @param interval query interval
     * @return A ReferenceBases containing the reference bases spanning the requested interval, cropped at the
     *         contig end if necessary
     */
    @Override
    public ReferenceBases getReferenceBases(SimpleInterval interval) throws IOException {
        final SimpleInterval queryInterval = cropIntervalAtContigEnd(interval);
        final String bases = twoBitFile.extract(simpleIntervalToReferenceRegion(queryInterval));
        return new ReferenceBases(bases.getBytes(), queryInterval);
    }

    @Override
    public SAMSequenceDictionary getReferenceSequenceDictionary(SAMSequenceDictionary optReadSequenceDictionaryToMatch) throws IOException {
	return this.twoBitSeqDictionary.toSAMSequenceDictionary();
    }

    public static boolean isTwoBit(String file) {
        return file.endsWith(TWO_BIT_EXTENSION);
    }

    private static ReferenceRegion simpleIntervalToReferenceRegion(SimpleInterval interval) {
        // ADAM and GA4GH both use zero-based half-open intervals for genome coordinates
        String contig = interval.getContig();
        long start = interval.getGA4GHStart();
        long end = interval.getGA4GHEnd();
        return new ReferenceRegion(contig, start, end, Strand.INDEPENDENT);
    }

    private SimpleInterval cropIntervalAtContigEnd( final SimpleInterval interval ) {
        // The 2bit query API does not support queries beyond the ends of contigs, so we need
        // to truncate our interval at the contig end if necessary.
        final Option<SequenceRecord> optContigRecord = twoBitSeqDictionary.apply(interval.getContig());
	if (optContigRecord.isEmpty()) {
	    throw new IllegalArgumentException("Contig " + interval.getContig() + " not found in reference dictionary");
	}
	final SequenceRecord contigRecord = optContigRecord.get();
        return new SimpleInterval(interval.getContig(), interval.getStart(), Math.min(interval.getEnd(), Math.toIntExact(contigRecord.length())));
    }

}
