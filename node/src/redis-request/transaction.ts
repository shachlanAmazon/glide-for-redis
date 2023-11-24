// automatically generated by the FlatBuffers compiler, do not modify

import * as flatbuffers from 'flatbuffers';

import { Command } from '../redis-request/command.js';


export class Transaction {
  bb: flatbuffers.ByteBuffer|null = null;
  bb_pos = 0;
  __init(i:number, bb:flatbuffers.ByteBuffer):Transaction {
  this.bb_pos = i;
  this.bb = bb;
  return this;
}

static getRootAsTransaction(bb:flatbuffers.ByteBuffer, obj?:Transaction):Transaction {
  return (obj || new Transaction()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

static getSizePrefixedRootAsTransaction(bb:flatbuffers.ByteBuffer, obj?:Transaction):Transaction {
  bb.setPosition(bb.position() + flatbuffers.SIZE_PREFIX_LENGTH);
  return (obj || new Transaction()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

commands(index: number, obj?:Command):Command|null {
  const offset = this.bb!.__offset(this.bb_pos, 4);
  return offset ? (obj || new Command()).__init(this.bb!.__indirect(this.bb!.__vector(this.bb_pos + offset) + index * 4), this.bb!) : null;
}

commandsLength():number {
  const offset = this.bb!.__offset(this.bb_pos, 4);
  return offset ? this.bb!.__vector_len(this.bb_pos + offset) : 0;
}

static startTransaction(builder:flatbuffers.Builder) {
  builder.startObject(1);
}

static addCommands(builder:flatbuffers.Builder, commandsOffset:flatbuffers.Offset) {
  builder.addFieldOffset(0, commandsOffset, 0);
}

static createCommandsVector(builder:flatbuffers.Builder, data:flatbuffers.Offset[]):flatbuffers.Offset {
  builder.startVector(4, data.length, 4);
  for (let i = data.length - 1; i >= 0; i--) {
    builder.addOffset(data[i]!);
  }
  return builder.endVector();
}

static startCommandsVector(builder:flatbuffers.Builder, numElems:number) {
  builder.startVector(4, numElems, 4);
}

static endTransaction(builder:flatbuffers.Builder):flatbuffers.Offset {
  const offset = builder.endObject();
  builder.requiredField(offset, 4) // commands
  return offset;
}

static createTransaction(builder:flatbuffers.Builder, commandsOffset:flatbuffers.Offset):flatbuffers.Offset {
  Transaction.startTransaction(builder);
  Transaction.addCommands(builder, commandsOffset);
  return Transaction.endTransaction(builder);
}
}
