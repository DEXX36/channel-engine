/*
 * Reference implementation of Channel Engine library
 */

import { ChannelEngine, ChannelEngineOpts, 
  IAssetManager, IChannelManager, 
  VodRequest, VodResponse, Channel, ChannelProfile,
  AudioTracks
} from "./index";

class RefAssetManager implements IAssetManager {
  private assets;
  private pos;
  constructor(opts?) {
    this.assets = {
      1: [
        {
          id: 1,
          title: "Sollevante",
          uri: "https://testcontent.eyevinn.technology/dolby/index.m3u8",
        }
      ],
    };
    this.pos = {
      1: 0,
    };
  }

  /**
   *
   * @param {Object} vodRequest
   *   {
   *      sessionId,
   *      category,
   *      playlistId
   *   }
   */
  getNextVod(vodRequest: VodRequest): Promise<VodResponse> {
    console.log(this.assets);
    return new Promise((resolve, reject) => {
      const channelId = vodRequest.playlistId;
      if (this.assets[channelId]) {
        let vod = this.assets[channelId][this.pos[channelId]++];
        if (this.pos[channelId] > this.assets[channelId].length - 1) {
          this.pos[channelId] = 0;
        }
        const vodResponse = {
          id: vod.id,
          title: vod.title,
          uri: vod.uri,
        };
        resolve(vodResponse);
      } else {
        reject("Invalid channelId provided");
      }
    });
  }
}

class RefChannelManager implements IChannelManager {
  getChannels(): Channel[] {
    //return [ { id: '1', profile: this._getProfile() }, { id: 'faulty', profile: this._getProfile() } ];
    return [{ 
      id: "1", 
      profile: this._getProfile(),
      audioTracks: this._getAudioTracks(),
    }];
  }

  _getProfile(): ChannelProfile[] {
    return [
      { resolution: [1280, 720], bw: 3725519, codecs: "avc1.64001F,mp4a.40.2" },
      { resolution: [1280, 720], bw: 5903428, codecs: "avc1.64001F,ac-3" },
      { resolution: [1280, 720], bw: 6676458, videoRange: "PQ", frameRate: 24.000, codecs: "hvc1.2.4.L93.90,mp4a.40.2" },
    ];
  }

  _getAudioTracks(): AudioTracks[] {
    return [
      { language: "ja", "name": "日本語", channels: 2, codecs: "mp4a.40.2", default: true },
      { language: "ja", "name": "日本語", channels: 6, codecs: "ac-3", default: true }
    ];
  }
}

const refAssetManager = new RefAssetManager();
const refChannelManager = new RefChannelManager();

const engineOptions: ChannelEngineOpts = {
  heartbeat: "/",
  useDemuxedAudio: true,
  averageSegmentDuration: 2000,
  channelManager: refChannelManager,
  slateRepetitions: 10,
  redisUrl: process.env.REDIS_URL,
};

const engine = new ChannelEngine(refAssetManager, engineOptions);
engine.start();
engine.listen(process.env.PORT || 8000);