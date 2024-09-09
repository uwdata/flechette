import { MessageHeader, Version } from '../constants.js';

export function writeMessage(builder, headerType, headerOffset, bodyLength, blocks) {
  builder.finish(
    builder.addObject(5, b => {
      b.addInt16(0, Version.V5, Version.V1);
      b.addInt8(1, headerType, MessageHeader.NONE);
      b.addOffset(2, headerOffset, 0);
      b.addInt64(3, bodyLength, 0);
      // NOT SUPPORTED: 4, message-level metadata
    })
  );

  const prefixSize = 8; // continuation indicator + message size
  const messageSize = builder.offset();
  const alignedSize = (messageSize + prefixSize + 7) & ~7;

  // track blocks for file footer
  blocks?.push({
    offset: builder.outputBytes,
    metadataLength: alignedSize,
    bodyLength
  });

  // write size prefix (including padding)
  builder.addInt32(alignedSize - prefixSize);

  // write the stream continuation indicator
  builder.addInt32(-1);

  // flush the builder content
  builder.flush();

  // add alignment padding as needed
  builder.addPadding(alignedSize - messageSize - prefixSize);
}
