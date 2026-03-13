/**
 * Regression tests for Escalation Monitor duplicate country rows.
 *
 * Root cause: the escalation adapter collected signals from 3 sources with
 * inconsistent country formats: protests used full names ("Iran") from ACLED,
 * outages used full names from proto, and news clusters used ISO2 codes ("IR")
 * from matchCountryNamesInText(). The correlation engine's clusterByCountry()
 * groups by raw string, so "Iran" !== "IR" produced separate rows.
 *
 * Fix: normalizeToCode() in escalation.ts converts all country values to ISO2
 * before pushing signals. generateTitle() resolves ISO2 back to full names.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = resolve(__dirname, '..');

const readSrc = (relPath: string) => readFileSync(resolve(root, relPath), 'utf-8');

// ============================================================
// 1. Static analysis: source structure guarantees
// ============================================================

describe('escalation adapter — country normalization structure', () => {
  const src = readSrc('src/services/correlation-engine/adapters/escalation.ts');

  it('all signals.push() blocks use normalizedCountry, not raw country', () => {
    const pushBlocks = src.split('signals.push({');
    for (let i = 1; i < pushBlocks.length; i++) {
      const block = pushBlocks[i]!.split('}')[0]!;
      assert.match(
        block,
        /country:\s*normalizedCountry/,
        `signals.push() block #${i} must use normalizedCountry, not raw p.country/o.country/country`,
      );
    }
  });

  it('each signal source has a continue guard before push', () => {
    const guardPattern = /if\s*\(\s*!normalizedCountry\s*\)\s*continue/g;
    const matches = src.match(guardPattern);
    assert.ok(matches, 'must have normalizedCountry continue guards');
    assert.ok(
      matches.length >= 3,
      `expected at least 3 continue guards (one per source), found ${matches.length}`,
    );
  });

  it('generateTitle resolves ISO2 via getCountryNameByCode', () => {
    const titleFn = src.slice(src.indexOf('generateTitle'));
    assert.match(
      titleFn,
      /getCountryNameByCode\s*\(/,
      'generateTitle must call getCountryNameByCode to resolve ISO2 to full name',
    );
  });

  it('normalizeToCode is NOT exported', () => {
    assert.doesNotMatch(
      src,
      /export\s+(function|const)\s+normalizeToCode/,
      'normalizeToCode must be a module-private helper, not exported',
    );
    assert.match(
      src,
      /function\s+normalizeToCode/,
      'normalizeToCode function must exist',
    );
  });

  it('imports nameToCountryCode and getCountryNameByCode from country-geometry', () => {
    assert.match(src, /nameToCountryCode/, 'must import nameToCountryCode');
    assert.match(src, /getCountryNameByCode/, 'must import getCountryNameByCode');
    assert.match(src, /iso3ToIso2Code/, 'must import iso3ToIso2Code');
  });
});
