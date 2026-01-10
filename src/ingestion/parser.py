"""
Docling-based PDF parser for Quanta-RAG
Extracts structured text from scientific papers
"""

import json
from pathlib import Path
from typing import Dict, List, Optional
from loguru import logger


class DoclingParser:
    """
    Advanced PDF parser using Docling
    Preserves document structure (headers, sections, body text)
    """
    
    def __init__(self):
        """Initialize Docling parser"""
        try:
            from docling.document_converter import DocumentConverter
            self.converter = DocumentConverter()
            logger.info("DoclingParser initialized successfully")
        except ImportError as e:
            logger.error(f"Failed to import Docling: {e}")
            logger.warning("Falling back to simple text extraction")
            self.converter = None
    
    def parse_pdf(self, pdf_path: str) -> Dict[str, any]:
        """
        Parse PDF and extract structured content
        
        Args:
            pdf_path: Path to PDF file
        
        Returns:
            Dictionary with:
                - full_text: Complete text content
                - sections: List of section dictionaries
                - metadata: Additional document metadata
        """
        pdf_file = Path(pdf_path)
        
        if not pdf_file.exists():
            logger.error(f"PDF file not found: {pdf_path}")
            return self._empty_result()
        
        # Try Docling parsing first
        if self.converter:
            try:
                return self._parse_with_docling(pdf_file)
            except Exception as e:
                logger.error(f"Docling parsing failed for {pdf_path}: {e}")
                logger.warning("Falling back to simple extraction")
        
        # Fallback to simple extraction
        return self._parse_with_fallback(pdf_file)
    
    def _parse_with_docling(self, pdf_file: Path) -> Dict[str, any]:
        """
        Parse PDF using Docling (preserves structure)
        
        Args:
            pdf_file: Path object to PDF
        
        Returns:
            Structured content dictionary
        """
        logger.info(f"Parsing with Docling: {pdf_file.name}")
        
        # Convert PDF to structured document
        result = self.converter.convert(str(pdf_file))
        
        # Extract sections with structure
        sections = []
        full_text_parts = []
        
        # Iterate through document elements
        for item in result.document.iterate_items():
            # Get element text and type
            text = item.text if hasattr(item, 'text') else str(item)
            element_type = item.label if hasattr(item, 'label') else 'body'
            
            # Add to sections if it's a heading
            if element_type in ['title', 'section_header', 'heading']:
                sections.append({
                    'type': element_type,
                    'text': text,
                    'level': getattr(item, 'level', 1)
                })
            
            # Add all text to full_text
            full_text_parts.append(text)
        
        full_text = '\n\n'.join(full_text_parts)
        
        logger.info(
            f"Docling extraction complete: {len(full_text)} chars, "
            f"{len(sections)} sections"
        )
        
        return {
            'full_text': full_text,
            'sections': sections,
            'metadata': {
                'parser': 'docling',
                'page_count': len(result.document.pages) if hasattr(result.document, 'pages') else 0,
                'element_count': len(list(result.document.iterate_items()))
            }
        }
    
    def _parse_with_fallback(self, pdf_file: Path) -> Dict[str, any]:
        """
        Simple fallback PDF extraction using pypdf
        
        Args:
            pdf_file: Path object to PDF
        
        Returns:
            Basic text extraction
        """
        logger.info(f"Using fallback parser for: {pdf_file.name}")
        
        try:
            from pypdf import PdfReader
            
            reader = PdfReader(str(pdf_file))
            full_text_parts = []
            
            for page in reader.pages:
                text = page.extract_text()
                if text:
                    full_text_parts.append(text)
            
            full_text = '\n\n'.join(full_text_parts)
            
            logger.info(
                f"Fallback extraction complete: {len(full_text)} chars, "
                f"{len(reader.pages)} pages"
            )
            
            return {
                'full_text': full_text,
                'sections': [],  # No structure preservation in fallback
                'metadata': {
                    'parser': 'pypdf_fallback',
                    'page_count': len(reader.pages)
                }
            }
            
        except Exception as e:
            logger.error(f"Fallback parsing failed: {e}")
            return self._empty_result()
    
    def _empty_result(self) -> Dict[str, any]:
        """Return empty result structure"""
        return {
            'full_text': '',
            'sections': [],
            'metadata': {
                'parser': 'none',
                'error': 'Failed to parse PDF'
            }
        }


def parse_pdf_safe(pdf_path: str) -> Dict[str, any]:
    """
    Convenience function for safe PDF parsing
    
    Args:
        pdf_path: Path to PDF file
    
    Returns:
        Parsed content dictionary
    """
    parser = DoclingParser()
    return parser.parse_pdf(pdf_path)


if __name__ == "__main__":
    # Test parser
    import sys
    
    if len(sys.argv) > 1:
        pdf_path = sys.argv[1]
        result = parse_pdf_safe(pdf_path)
        
        print(f"\nParser: {result['metadata']['parser']}")
        print(f"Text length: {len(result['full_text'])} characters")
        print(f"Sections: {len(result['sections'])}")
        
        if result['sections']:
            print("\nSections:")
            for section in result['sections'][:5]:  # Show first 5
                print(f"  - {section['type']}: {section['text'][:50]}...")
    else:
        print("Usage: python parser.py <path_to_pdf>")
