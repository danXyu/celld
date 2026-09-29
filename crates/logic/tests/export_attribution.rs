use celld_logic::export::{
    frames_through, shed, Attribution, Capture, CapturedWal, Released, Shed, WalGeneration,
    WalPoint, MAX_RETAINED_CAPTURES,
};

const PAGE: u32 = 4096;
const FRAME: u64 = PAGE as u64 + 24;

fn generation(salt1: u32) -> WalGeneration {
    WalGeneration {
        salt1,
        salt2: salt1.wrapping_mul(2_654_435_761),
    }
}

fn at(salt1: u32, frames: u64) -> WalPoint {
    WalPoint {
        generation: generation(salt1),
        frames,
    }
}

/// A capture of frames `(after, through]` of a generation.
fn partial(txid: u64, salt1: u32, after: u64, through: u64) -> Capture {
    Capture {
        txid,
        page_size: PAGE,
        full_image: false,
        wal: Some(CapturedWal {
            generation: generation(salt1),
            offset: 32 + after * FRAME,
            size: (through - after) * FRAME,
        }),
    }
}

/// A full image read at frame `through` of a generation.
fn image(txid: u64, salt1: u32, through: u64) -> Capture {
    Capture {
        txid,
        page_size: PAGE,
        full_image: true,
        wal: Some(CapturedWal {
            generation: generation(salt1),
            offset: 32,
            size: through * FRAME,
        }),
    }
}

fn commits(released: Vec<Released<u32>>) -> Vec<(u64, u32)> {
    released
        .into_iter()
        .map(|entry| match entry {
            Released::Commit { label, payload } => (label, payload),
            gap => panic!("unexpected {gap:?}"),
        })
        .collect()
}

#[test]
fn frame_arithmetic_counts_whole_frames() {
    assert_eq!(frames_through(32, PAGE), 0);
    assert_eq!(frames_through(0, PAGE), 0);
    assert_eq!(frames_through(32 + FRAME - 1, PAGE), 0);
    assert_eq!(frames_through(32 + FRAME, PAGE), 1);
    assert_eq!(frames_through(32 + 7 * FRAME, PAGE), 7);
    assert_eq!(frames_through(32 + 3 * (512 + 24), 512), 3);
}

#[test]
fn a_commit_is_released_only_once_its_capture_is_proven() {
    let mut export = Attribution::new();
    export.commit(at(1, 2), 10, 1);
    export.captured(&partial(1, 1, 0, 2));
    export.proven(0);
    assert!(export.take_released().is_empty());
    assert_eq!(export.released_position(), 0);
    export.proven(1);
    assert_eq!(commits(export.take_released()), [(1, 1)]);
    assert_eq!(export.released_position(), 1);
    assert_eq!(export.pending_bytes(), 0);
}

#[test]
fn a_commit_that_arrives_after_its_capture_gets_that_capture() {
    let mut export = Attribution::new();
    export.captured(&partial(1, 1, 0, 2));
    export.captured(&partial(2, 1, 2, 5));
    export.proven(2);
    // Nothing has arrived, so nothing may be claimed yet.
    assert_eq!(export.released_position(), 0);
    export.commit(at(1, 1), 1, 1);
    export.commit(at(1, 4), 1, 2);
    export.commit(at(1, 5), 1, 3);
    assert_eq!(commits(export.take_released()), [(1, 1), (2, 2), (2, 3)]);
    assert_eq!(export.released_position(), 2);
}

#[test]
fn one_capture_holds_several_commits_and_a_later_commit_waits() {
    let mut export = Attribution::new();
    export.commit(at(1, 1), 1, 1);
    export.commit(at(1, 3), 1, 2);
    export.commit(at(1, 4), 1, 3);
    export.captured(&partial(1, 1, 0, 3));
    export.proven(1);
    assert_eq!(commits(export.take_released()), [(1, 1), (1, 2)]);
    // Commit 3 is not captured yet, so the position stops before txid 1 only
    // if something labelled 1 could still arrive; commit 3 passed it.
    assert_eq!(export.released_position(), 1);
    export.captured(&partial(2, 1, 3, 4));
    export.proven(2);
    assert_eq!(commits(export.take_released()), [(2, 3)]);
    assert_eq!(export.released_position(), 2);
}

#[test]
fn a_capture_of_only_control_writes_needs_the_cell_to_catch_up() {
    let mut export = Attribution::new();
    export.commit(at(1, 2), 1, 1);
    // Frame 3 is the capture loop's own `_litestream_seq` write.
    export.captured(&partial(1, 1, 0, 3));
    export.proven(1);
    assert_eq!(commits(export.take_released()), [(1, 1)]);
    // An app commit ending at frame 3 could still be on its way.
    assert_eq!(export.released_position(), 0);
    export.caught_up();
    assert_eq!(export.released_position(), 1);
}

#[test]
fn the_released_position_never_passes_the_proof() {
    let mut export = Attribution::new();
    export.commit(at(1, 1), 1, 1);
    export.commit(at(1, 2), 1, 2);
    export.captured(&partial(1, 1, 0, 1));
    export.captured(&partial(2, 1, 1, 2));
    export.caught_up();
    export.proven(1);
    assert_eq!(commits(export.take_released()), [(1, 1)]);
    assert_eq!(export.released_position(), 1);
    export.proven(2);
    assert_eq!(commits(export.take_released()), [(2, 2)]);
    assert_eq!(export.released_position(), 2);
}

#[test]
fn a_commit_labelled_below_an_earlier_proof_is_released_at_once() {
    let mut export = Attribution::new();
    export.captured(&partial(1, 1, 0, 2));
    export.proven(1);
    export.commit(at(1, 2), 1, 7);
    assert_eq!(commits(export.take_released()), [(1, 7)]);
    assert_eq!(export.released_position(), 1);
}

#[test]
fn a_checkpoint_splits_captures_across_a_restart() {
    let mut export = Attribution::new();
    // Lead capture, a commit under the passive barrier, then the restart:
    // the capture loop writes frame 1 of the new generation itself and
    // captures from the header.
    export.commit(at(1, 3), 1, 1);
    export.captured(&partial(1, 1, 0, 3));
    export.commit(at(1, 4), 1, 2);
    export.captured(&partial(2, 1, 3, 4));
    export.commit(at(2, 2), 1, 3);
    export.captured(&partial(3, 2, 0, 2));
    export.proven(3);
    assert_eq!(commits(export.take_released()), [(1, 1), (2, 2), (3, 3)]);
    assert_eq!(export.released_position(), 3);
    assert_eq!(export.unmatched_total(), 0);
}

#[test]
fn a_full_image_holds_earlier_generations_and_its_own_boundary() {
    let mut export = Attribution::new();
    export.commit(at(1, 2), 1, 1);
    export.captured(&partial(1, 1, 0, 2));
    // Commits after the lead capture, backfilled by a TRUNCATE checkpoint
    // before any partial capture read them.
    export.commit(at(1, 3), 1, 2);
    // The new generation's salts carry no order: 900 is not 1 + 1.
    export.commit(at(900, 2), 1, 3);
    export.commit(at(900, 4), 1, 4);
    export.captured(&image(2, 900, 2));
    export.proven(2);
    assert_eq!(commits(export.take_released()), [(1, 1), (2, 2), (2, 3)]);
    // Commit 4 landed after the image's read and waits for the next capture.
    assert_eq!(export.released_position(), 2);
    export.captured(&partial(3, 900, 2, 4));
    export.proven(3);
    assert_eq!(commits(export.take_released()), [(3, 4)]);
}

#[test]
fn a_commit_in_an_unseen_generation_waits_for_a_capture_in_it() {
    let mut export = Attribution::new();
    export.captured(&image(1, 1, 2));
    // Commit 1 is in a generation no capture has read yet: it is later.
    export.commit(at(5, 1), 1, 1);
    export.proven(1);
    assert!(export.take_released().is_empty());
    // The image may yet turn out to hold commit 1, if a later commit shows
    // its generation came first, so the position cannot reach the image.
    export.caught_up();
    assert_eq!(export.released_position(), 0);
    export.captured(&partial(2, 5, 0, 1));
    export.proven(2);
    assert_eq!(commits(export.take_released()), [(2, 1)]);
    assert_eq!(export.released_position(), 2);
}

#[test]
fn a_commit_that_arrived_before_a_capture_is_in_its_generation_or_earlier() {
    let mut export = Attribution::new();
    // The first capture is an image of generation 2; commit 1 is in
    // generation 1, which no capture read. It arrived before the image was
    // reported, so it committed before, and the WAL does not restart between
    // a capture's read and its report: generation 1 came first.
    export.commit(at(1, 1), 1, 1);
    export.captured(&image(1, 2, 3));
    export.proven(1);
    assert_eq!(commits(export.take_released()), [(1, 1)]);
}

#[test]
fn the_next_capture_orders_a_commit_that_arrived_after_an_image() {
    let mut export = Attribution::new();
    export.commit(at(1, 1), 1, 1);
    export.captured(&partial(1, 1, 0, 1));
    export.captured(&image(2, 2, 3));
    // Commit 2 raced the image into generation 1, after the lead capture;
    // nothing yet says whether its generation precedes the image's.
    export.commit(at(1, 2), 1, 2);
    export.proven(2);
    assert_eq!(commits(export.take_released()), [(1, 1), (2, 2)]);
    // Here the capture chain already put generation 1 first. Without it,
    // the next capture of generation 2 would have.
    let mut export = Attribution::new();
    export.captured(&image(1, 2, 3));
    export.commit(at(1, 2), 1, 1);
    export.caught_up();
    export.proven(1);
    assert!(export.take_released().is_empty());
    export.captured(&partial(2, 2, 3, 4));
    assert_eq!(commits(export.take_released()), [(1, 1)]);
    export.proven(2);
    // Capture 2 may yet hold a commit on its way.
    assert_eq!(export.released_position(), 1);
    export.caught_up();
    assert_eq!(export.released_position(), 2);
}

#[test]
fn a_seed_covers_no_commit() {
    let mut export = Attribution::new();
    export.captured(&Capture {
        txid: 1,
        page_size: PAGE,
        full_image: true,
        wal: None,
    });
    export.commit(at(1, 1), 1, 1);
    export.captured(&partial(2, 1, 0, 1));
    export.proven(2);
    assert_eq!(commits(export.take_released()), [(2, 1)]);
    assert_eq!(export.released_position(), 2);
}

#[test]
fn a_commit_a_capture_passed_becomes_a_gap_released_with_the_proof() {
    let mut export = Attribution::new();
    export.commit(at(1, 1), 1, 1);
    export.captured(&partial(1, 1, 0, 1));
    // A capture that skips frames 2 and 3, then a commit ending in them.
    export.captured(&partial(2, 1, 3, 5));
    export.commit(at(1, 3), 5, 2);
    export.commit(at(1, 5), 1, 3);
    assert_eq!(export.unmatched_total(), 1);
    assert_eq!(export.pending_bytes(), 2);
    export.proven(1);
    assert_eq!(commits(export.take_released()), [(1, 1)]);
    // The dropped commit's true label is unknown, so the position waits for
    // its gap.
    assert_eq!(export.released_position(), 0);
    export.proven(2);
    assert_eq!(
        export.take_released(),
        [
            Released::Gap {
                after: 0,
                through: 2,
                unmatched: 1,
                overflowed: 0,
            },
            Released::Commit {
                label: 2,
                payload: 3,
            },
        ]
    );
    assert_eq!(export.released_position(), 2);
}

#[test]
fn a_commit_behind_a_later_generation_is_unmatched() {
    let mut export = Attribution::new();
    export.captured(&partial(1, 1, 0, 2));
    export.captured(&partial(2, 7, 0, 1));
    export.commit(at(1, 2), 1, 1);
    // The commit ending past the last capture of generation 1 arrives after
    // a partial capture of a later generation sealed it.
    export.commit(at(1, 3), 1, 2);
    export.commit(at(7, 1), 1, 3);
    assert_eq!(export.unmatched_total(), 1);
    export.proven(2);
    let released = export.take_released();
    assert_eq!(released.len(), 3);
    assert!(matches!(
        released[1],
        Released::Gap {
            through: 2,
            unmatched: 1,
            ..
        }
    ));
}

#[test]
fn over_budget_the_buffer_sheds_before_pending_commits() {
    assert_eq!(
        shed(100, 30, 50),
        Shed {
            buffered: 0,
            pending_limit: 30
        }
    );
    assert_eq!(
        shed(100, 30, 90),
        Shed {
            buffered: 20,
            pending_limit: 30
        }
    );
    assert_eq!(
        shed(100, 130, 10),
        Shed {
            buffered: 10,
            pending_limit: 100
        }
    );
    assert_eq!(
        shed(0, 5, 0),
        Shed {
            buffered: 0,
            pending_limit: 0
        }
    );
}

#[test]
fn shed_commits_become_one_gap_bounded_by_the_last_label() {
    let mut export = Attribution::new();
    export.commit(at(1, 1), 10, 1);
    export.commit(at(1, 2), 10, 2);
    export.commit(at(1, 3), 10, 3);
    export.commit(at(1, 4), 10, 4);
    export.captured(&partial(1, 1, 0, 1));
    export.captured(&partial(2, 1, 1, 2));
    export.shed_to(15);
    assert_eq!(export.pending_bytes(), 10);
    assert_eq!(export.pending_len(), 2);
    // Commit 3, shed before its capture, bounds the gap: it cannot be
    // released until that capture is known and proven.
    export.proven(2);
    assert!(export.take_released().is_empty());
    assert_eq!(export.released_position(), 0);
    export.captured(&partial(3, 1, 2, 4));
    export.proven(3);
    assert_eq!(
        export.take_released(),
        [
            Released::Gap {
                after: 0,
                through: 3,
                unmatched: 0,
                overflowed: 3,
            },
            Released::Commit {
                label: 3,
                payload: 4,
            },
        ]
    );
    assert_eq!(export.released_position(), 3);
}

#[test]
fn forgotten_captures_turn_a_late_commit_into_a_gap() {
    let mut export = Attribution::new();
    for txid in 1..=(MAX_RETAINED_CAPTURES as u64 + 1) {
        export.captured(&partial(txid, 1, txid - 1, txid));
    }
    export.commit(at(1, 1), 1, 1);
    assert_eq!(export.unmatched_total(), 1);
    export.commit(at(1, 2), 1, 2);
    assert_eq!(export.unmatched_total(), 1);
    export.proven(MAX_RETAINED_CAPTURES as u64 + 1);
    let released = export.take_released();
    assert!(matches!(released[0], Released::Gap { after: 0, .. }));
    assert_eq!(
        released[1],
        Released::Commit {
            label: 2,
            payload: 2
        }
    );
}

/// The deterministic interleaving: commits on the cell thread and captures
/// on the replication thread, in every order, through one FIFO.
mod interleaving {
    use super::*;

    #[derive(Clone, Copy, Debug)]
    enum Step {
        /// The app commits one to three frames.
        Write,
        /// The capture loop writes `_litestream_seq`.
        Control,
        /// A safe point: push every committed commit, then catch up.
        SafePoint,
        /// A sync capture.
        Capture,
        /// A passive checkpoint that restarts the WAL: lead capture, barrier
        /// capture, the control write in the new generation, and a partial
        /// capture from its header.
        Restart,
        /// A TRUNCATE checkpoint: lead capture, an app commit racing it into
        /// the old generation, then a boundary image of the new generation.
        Truncate,
        /// A full image of the current generation without a restart.
        Image,
        /// A proof of every capture written so far.
        Prove,
    }

    const STEPS: [Step; 8] = [
        Step::Write,
        Step::Control,
        Step::SafePoint,
        Step::Capture,
        Step::Restart,
        Step::Truncate,
        Step::Image,
        Step::Prove,
    ];

    enum Message {
        Commit(WalPoint, u32),
        CaughtUp,
        Captured(Capture),
        Proven(u64),
    }

    struct World {
        salt1: u32,
        next_salt: u32,
        frames: u64,
        captured_frames: u64,
        txid: u64,
        /// Every commit's point and true label, in commit order.
        truth: Vec<(WalPoint, Option<u64>)>,
        unpushed: Vec<u32>,
        fifo: Vec<Message>,
        write_size: u64,
    }

    impl World {
        fn new() -> Self {
            Self {
                salt1: 11,
                next_salt: 500,
                frames: 0,
                captured_frames: 0,
                txid: 0,
                truth: Vec::new(),
                unpushed: Vec::new(),
                fifo: Vec::new(),
                write_size: 0,
            }
        }

        fn write(&mut self) {
            self.write_size = self.write_size % 3 + 1;
            self.frames += self.write_size;
            let point = at(self.salt1, self.frames);
            self.unpushed.push(self.truth.len() as u32);
            self.truth.push((point, None));
        }

        fn safe_point(&mut self) {
            for index in std::mem::take(&mut self.unpushed) {
                let point = self.truth[index as usize].0;
                self.fifo.push(Message::Commit(point, index));
            }
            self.fifo.push(Message::CaughtUp);
        }

        fn label(&mut self, covers: impl Fn(WalPoint) -> bool) {
            let txid = self.txid;
            for (point, label) in &mut self.truth {
                if label.is_none() && covers(*point) {
                    *label = Some(txid);
                }
            }
        }

        fn capture(&mut self) {
            if self.frames == self.captured_frames {
                return;
            }
            self.txid += 1;
            let (salt1, after, through) = (self.salt1, self.captured_frames, self.frames);
            self.label(|point| point.generation == generation(salt1) && point.frames <= through);
            self.fifo
                .push(Message::Captured(partial(self.txid, salt1, after, through)));
            self.captured_frames = self.frames;
        }

        fn image(&mut self) {
            self.txid += 1;
            // Every commit so far is in the image, whatever its generation.
            self.label(|_| true);
            self.fifo
                .push(Message::Captured(image(self.txid, self.salt1, self.frames)));
            self.captured_frames = self.frames;
        }

        fn new_generation(&mut self) {
            // Salts of a new generation are unordered, as when SQLite
            // re-randomizes them.
            self.next_salt = self
                .next_salt
                .wrapping_mul(1_103_515_245)
                .wrapping_add(12_345);
            self.salt1 = self.next_salt;
            self.frames = 1;
            self.captured_frames = 0;
        }

        fn step(&mut self, step: Step) {
            match step {
                Step::Write => self.write(),
                Step::Control => self.frames += 1,
                Step::SafePoint => self.safe_point(),
                Step::Capture => self.capture(),
                Step::Restart => {
                    self.capture();
                    self.capture();
                    self.new_generation();
                    self.capture();
                }
                Step::Truncate => {
                    self.capture();
                    self.write();
                    self.new_generation();
                    self.image();
                }
                Step::Image => self.image(),
                Step::Prove => self.fifo.push(Message::Proven(self.txid)),
            }
        }

        fn finish(&mut self) {
            self.safe_point();
            self.step(Step::Control);
            self.capture();
            self.safe_point();
            self.fifo.push(Message::Proven(self.txid));
        }
    }

    fn run(steps: &[Step]) {
        let mut world = World::new();
        for step in steps {
            world.step(*step);
        }
        world.finish();
        let mut export = Attribution::new();
        let mut released: Vec<(u64, u32)> = Vec::new();
        let mut proven = 0;
        for message in std::mem::take(&mut world.fifo) {
            match message {
                Message::Commit(point, index) => export.commit(point, 1, index),
                Message::CaughtUp => export.caught_up(),
                Message::Captured(capture) => export.captured(&capture),
                Message::Proven(txid) => {
                    proven = txid;
                    export.proven(txid)
                }
            }
            for entry in export.take_released() {
                match entry {
                    Released::Commit { label, payload } => released.push((label, payload)),
                    gap => panic!("{steps:?}: {gap:?}"),
                }
            }
            let position = export.released_position();
            assert!(
                position <= proven,
                "{steps:?}: position {position} past proof"
            );
            // Every commit labelled at or below the position is out.
            for (index, (_, label)) in world.truth.iter().enumerate() {
                if label.is_some_and(|label| label <= position) {
                    assert!(
                        released
                            .iter()
                            .any(|(_, released)| *released == index as u32),
                        "{steps:?}: position {position} claims commit {index} unreleased"
                    );
                }
            }
        }
        assert_eq!(export.unmatched_total(), 0, "{steps:?}");
        let expected: Vec<(u64, u32)> = world
            .truth
            .iter()
            .enumerate()
            .map(|(index, (_, label))| (label.expect("finish captures all"), index as u32))
            .collect();
        assert_eq!(released, expected, "{steps:?}");
        assert_eq!(export.released_position(), world.txid, "{steps:?}");
        assert_eq!(export.pending_len(), 0, "{steps:?}");
    }

    #[test]
    fn every_order_of_five_steps() {
        let mut steps = [Step::Write; 5];
        for code in 0..STEPS.len().pow(5) {
            let mut rest = code;
            for step in &mut steps {
                *step = STEPS[rest % STEPS.len()];
                rest /= STEPS.len();
            }
            run(&steps);
        }
    }

    #[test]
    fn long_seeded_orders() {
        let mut seed: u64 = 0x5eed;
        for _ in 0..3_000 {
            let steps: Vec<Step> = (0..40)
                .map(|_| {
                    seed = seed
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1_442_695_040_888_963_407);
                    STEPS[(seed >> 33) as usize % STEPS.len()]
                })
                .collect();
            run(&steps);
        }
    }
}

#[test]
fn an_unplaced_commit_becomes_a_gap_bounded_by_the_latest_capture() {
    let mut state = Attribution::new();
    state.commit(at(1, 2), 10, 1u32);
    state.captured(&partial(1, 1, 0, 2));
    state.captured(&partial(2, 1, 2, 4));
    // The WAL hook lost the race with a restart: the capture holding the
    // commit is one of those already reported.
    state.unplaced(10, 2);
    state.commit(at(1, 5), 10, 3);
    state.captured(&partial(3, 1, 4, 5));
    state.proven(3);
    let released = state.take_released();
    assert_eq!(
        released,
        vec![
            Released::Commit {
                label: 1,
                payload: 1
            },
            // The gap starts at the released position before this release.
            Released::Gap {
                after: 0,
                through: 2,
                unmatched: 1,
                overflowed: 0
            },
            Released::Commit {
                label: 3,
                payload: 3
            },
        ]
    );
    assert_eq!(state.unmatched_total(), 1);
    assert_eq!(state.pending_bytes(), 0);
}

#[test]
fn a_commit_shed_before_it_arrived_is_a_gap_in_its_place() {
    let mut state = Attribution::new();
    state.commit(at(1, 1), 10, 1u32);
    state.dropped(at(1, 2));
    state.dropped(at(1, 3));
    state.commit(at(1, 5), 10, 4);
    // Nothing is labelled yet, so nothing is released even with a proof.
    state.proven(10);
    assert_eq!(state.take_released(), vec![]);
    state.captured(&partial(1, 1, 0, 2));
    state.captured(&partial(2, 1, 2, 5));
    assert_eq!(
        state.take_released(),
        vec![
            Released::Commit {
                label: 1,
                payload: 1
            },
            // The later shed commit's label bounds the merged gap.
            Released::Gap {
                after: 0,
                through: 2,
                unmatched: 0,
                overflowed: 2
            },
            Released::Commit {
                label: 2,
                payload: 4
            },
        ]
    );
    assert_eq!(state.pending_len(), 0);
}
